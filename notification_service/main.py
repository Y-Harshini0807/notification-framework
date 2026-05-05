from fastapi import FastAPI, HTTPException, Request, Depends, Header, Response, UploadFile, File, Query, Form
from pydantic import BaseModel
from pymongo import MongoClient
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List
import os
import uuid
import hmac
import hashlib
import json
import threading
import secrets
import time
import math
import re
import mimetypes
from pathlib import Path
from collections import defaultdict
from urllib.parse import urlparse
import ipaddress
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse
from fastapi.responses import RedirectResponse
from celery_config import celery_app
from tasks import send_notification
from dlq_processor import run_worker
from kombu import Connection, Queue
import requests as http_requests

load_dotenv()

# ── DLQ processor runs in a background daemon thread ─────────────────────────
threading.Thread(target=run_worker, daemon=True).start()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)

db = client["notification_db"]
notification_requests_collection = db["notification_requests"]
notification_jobs_collection     = db["notification_jobs"]
preferences_collection           = db["user_preferences"]
delivery_logs_collection         = db["delivery_logs"]
rate_limit_logs_collection       = db["rate_limit_logs"]
providers_collection             = db["providers"]
dlq_collection                   = db["dlq"]
clients_collection               = db["clients"]
global_rate_limit_logs_collection = db["global_rate_limit_logs"]
queue_controls_collection        = db["queue_controls"]
webhook_calls_collection        = db["webhook_calls"]
ultramsg_inbound_collection     = db["ultramsg_inbound"]
media_files_collection         = db["media_files"]

MEDIA_DIR = Path(os.getenv("MEDIA_DIR", os.path.join(os.path.dirname(__file__), "media_store"))).resolve()
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").rstrip("/")
MEDIA_TOKEN_SECRET = os.getenv("MEDIA_TOKEN_SECRET", "")
MEDIA_URL_TTL_DAYS = max(1, int(os.getenv("MEDIA_URL_TTL_DAYS", "30")))
ALLOW_PRIVATE_MEDIA_URLS = str(os.getenv("ALLOW_PRIVATE_MEDIA_URLS", "0")).strip().lower() in {"1", "true", "yes"}
ALLOWED_PRIVATE_MEDIA_HOSTS = {
    h.strip().lower()
    for h in (os.getenv("ALLOWED_PRIVATE_MEDIA_HOSTS", "") or "").split(",")
    if h.strip()
}
CHANNELS = ("email", "sms", "whatsapp", "push")
QUEUE_FAILOVER_ENABLED = str(os.getenv("QUEUE_FAILOVER_ENABLED", "1")).strip().lower() in {"1", "true", "yes"}
PRIMARY_QUEUE_CONGESTION_DEPTH = max(1, int(os.getenv("PRIMARY_QUEUE_CONGESTION_DEPTH", "100")))
PRIMARY_QUEUE_MAX_BUFFER = max(PRIMARY_QUEUE_CONGESTION_DEPTH, int(os.getenv("PRIMARY_QUEUE_MAX_BUFFER", "500")))
PRIMARY_QUEUE_RECOVER_DEPTH = max(0, int(os.getenv("PRIMARY_QUEUE_RECOVER_DEPTH", str(max(10, PRIMARY_QUEUE_CONGESTION_DEPTH // 2)))))
PRIMARY_QUEUE_STUCK_SECONDS = max(5, int(os.getenv("PRIMARY_QUEUE_STUCK_SECONDS", "60")))
QUEUE_FAILOVER_MONITOR_INTERVAL_SEC = max(2, int(os.getenv("QUEUE_FAILOVER_MONITOR_INTERVAL_SEC", "5")))
QUEUE_FAILOVER_MIN_SWITCH_SECONDS = max(5, int(os.getenv("QUEUE_FAILOVER_MIN_SWITCH_SECONDS", "30")))


def _guess_mime(filename: str, fallback: str = "application/octet-stream") -> str:
    mt, _ = mimetypes.guess_type(filename or "")
    return mt or fallback


def _public_url_for(request: Request, path: str) -> str:
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}{path}"
    # request.base_url already has trailing slash
    return str(request.base_url).rstrip("/") + path


def _build_media_url(request: Request, doc: dict, include_token: bool = True) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    source_url = doc.get("source_url")
    if source_url:
        return str(source_url)

    file_id = doc.get("file_id")
    if not file_id:
        return None

    path = f"/media/{file_id}"
    token = doc.get("access_token")
    if include_token and token:
        path = f"{path}?token={token}"
    return _public_url_for(request, path)


def _refresh_media_doc_if_expired(doc: dict) -> dict:
    """
    Extend media accessibility window by rotating token and TTL when expired.
    Keeps file_id stable across retries/DLQ replays while allowing URL refresh.
    """
    if not isinstance(doc, dict):
        return doc
    file_id = doc.get("file_id")
    expires_at = doc.get("expires_at")
    now = datetime.utcnow()
    if not file_id or not isinstance(expires_at, datetime) or expires_at > now:
        return doc

    updates = {
        "expires_at": now + timedelta(days=MEDIA_URL_TTL_DAYS),
        "updated_at": now,
    }
    if doc.get("stored_path"):
        updates["access_token"] = secrets.token_urlsafe(24)
    media_files_collection.update_one({"file_id": str(file_id)}, {"$set": updates})
    return {**doc, **updates}


def _is_publicly_reachable_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url).strip())
    except Exception:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        return False
    if hostname in ALLOWED_PRIVATE_MEDIA_HOSTS:
        return True
    if ALLOW_PRIVATE_MEDIA_URLS:
        return True
    if hostname in {"localhost", "0.0.0.0"} or hostname.endswith(".local"):
        return False

    try:
        addr = ipaddress.ip_address(hostname)
    except ValueError:
        # Domain names are treated as potentially public.
        return True

    return not (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    )


def _normalize_attachments(request: Request, content: dict) -> dict:
    """
    Accept attachments inside content in either of these shapes:
      - {"attachments": [{"file_id": "..."}]}
      - {"attachments": [{"url": "https://...", "name": "...", "mime_type": "...", "size_bytes": 123}]}

    Produces:
      content["attachments"] = [{url,name,mime_type,size_bytes,file_id}, ...]
    """
    content = dict(content or {})
    atts = content.get("attachments")
    if not isinstance(atts, list):
        return content

    normalized = []
    for a in atts:
        if not isinstance(a, dict):
            continue
        file_id = a.get("file_id")
        url = a.get("url")
        name = a.get("name") or a.get("filename")
        mime_type = a.get("mime_type") or (a.get("content_type"))
        size_bytes = a.get("size_bytes")

        if file_id:
            doc = media_files_collection.find_one({"file_id": str(file_id)}, {"_id": 0})
            if doc:
                doc = _refresh_media_doc_if_expired(doc)
                url = _build_media_url(request, doc, include_token=True)
                name = name or doc.get("original_name")
                mime_type = mime_type or doc.get("mime_type")
                size_bytes = size_bytes or doc.get("size_bytes")
                if a.get("delivery_mode") is None and doc.get("delivery_mode"):
                    a = {**a, "delivery_mode": doc.get("delivery_mode")}

        if url:
            normalized.append({
                "file_id": (str(file_id) if file_id else None),
                "url": str(url),
                "name": str(name) if name else None,
                "mime_type": str(mime_type) if mime_type else _guess_mime(name or ""),
                "size_bytes": int(size_bytes) if isinstance(size_bytes, (int, float)) else None,
                "delivery_mode": str(a.get("delivery_mode")) if a.get("delivery_mode") else None,
            })

    content["attachments"] = normalized
    return content


def _validate_channel_attachments(channel: str, content: dict) -> None:
    """
    WhatsApp providers fetch media from the attachment URL themselves.
    Localhost/private URLs work in local testing for upload, but not from the provider's servers.
    """
    normalized_channel = (channel or "").strip().lower()
    if normalized_channel != "whatsapp":
        return

    invalid_targets = []
    for a in (content.get("attachments") or []):
        if not isinstance(a, dict):
            continue
        url = a.get("url")
        if not url:
            continue
        if _is_publicly_reachable_url(url):
            continue
        invalid_targets.append({
            "file_id": a.get("file_id"),
            "url": str(url),
            "name": a.get("name"),
        })

    if invalid_targets:
        first = invalid_targets[0]
        attachment_label = first.get("file_id") or first.get("name") or first["url"]
        raise HTTPException(
            status_code=400,
            detail=(
                "WhatsApp attachments must use a publicly reachable URL. "
                f"Attachment '{attachment_label}' resolved to '{first['url']}', which is not public. "
                "Set PUBLIC_BASE_URL to a public HTTPS host (for example an ngrok URL) before using "
                "local uploaded files as WhatsApp attachments."
            ),
        )


def _emit_client_webhook(client_id: str, event: dict) -> None:
    """
    Best-effort webhook dispatch to the client's configured webhook_url.
    Never raises.
    """
    try:
        cid = (client_id or "").strip()
        if not cid:
            return
        client_doc = clients_collection.find_one({"client_id": cid}, {"_id": 0, "webhook_url": 1})
        url = (client_doc or {}).get("webhook_url")
        if not url:
            return

        started_at = datetime.utcnow()
        try:
            resp = http_requests.post(url, json=event, timeout=5)
            ok = 200 <= resp.status_code < 300
            webhook_calls_collection.insert_one({
                "call_id":    f"wh_{uuid.uuid4().hex[:12]}",
                "client_id":  cid,
                "url":        url,
                "status":     "SUCCESS" if ok else "FAILED",
                "http_status": resp.status_code,
                "response_body": (resp.text[:2000] if resp.text else None),
                "event":      event,
                "created_at": started_at,
            })
        except Exception as e:
            webhook_calls_collection.insert_one({
                "call_id":    f"wh_{uuid.uuid4().hex[:12]}",
                "client_id":  cid,
                "url":        url,
                "status":     "ERROR",
                "error":      str(e),
                "event":      event,
                "created_at": started_at,
            })
    except Exception:
        return


def _percentile(sorted_values: list[int], p: float) -> Optional[int]:
    """
    Nearest-rank percentile over a pre-sorted list.
    p in [0,100].
    """
    if not sorted_values:
        return None
    if p <= 0:
        return int(sorted_values[0])
    if p >= 100:
        return int(sorted_values[-1])
    k = int(math.ceil((p / 100.0) * len(sorted_values))) - 1
    k = max(0, min(k, len(sorted_values) - 1))
    return int(sorted_values[k])


def _require_bearer_token(authorization: Optional[str] = Header(default=None)):
    """
    UI sends Authorization: Bearer <token>.
    We only require presence (admin auth is handled by the login service).
    """
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing Authorization Bearer token")
    return token


def _normalize_channel(queue_label: str) -> str:
    """
    Accepts UI values like 'EMAIL'/'WHATSAPP'/'SMS' or queue names like 'whatsapp-notify-q'.
    Returns canonical channel: email|sms|whatsapp|push
    """
    raw = (queue_label or "").strip().lower()
    if raw in {"email", "sms", "whatsapp", "push"}:
        return raw
    if "-notify-q-backup" in raw:
        return raw.split("-notify-q-backup", 1)[0]
    if raw.endswith("-notify-q"):
        return raw.replace("-notify-q", "")
    # UI dropdown uses uppercase labels
    if raw in {"email", "e-mail"}:
        return "email"
    if raw in {"wa", "whatsapp"}:
        return "whatsapp"
    if raw in {"sms", "text"}:
        return "sms"
    if raw in {"push", "fcm"}:
        return "push"
    # Fall back (keeps behavior predictable)
    return raw or "email"


def _channel_to_queue_name(channel: str) -> str:
    channel = (channel or "").strip().lower()
    return f"{channel}-notify-q"


def _channel_to_backup_queue_name(channel: str) -> str:
    channel = (channel or "").strip().lower()
    return f"{channel}-notify-q-backup"


def _queue_role(queue_name: str) -> str:
    raw = (queue_name or "").strip().lower()
    if "-notify-q-backup" in raw:
        return "backup"
    return "primary"


def _default_queue_control(channel: str) -> dict:
    return {
        "channel": channel,
        "paused": False,
        # rate_limit is stored as jobs/min (0 means "unset" -> fall back to hardcoded RATE_LIMITS)
        "rate_limit": 0,
        "active_queue": _channel_to_queue_name(channel),
        "backup_queue": None,
        "backup_enabled": True,
        "congestion_depth": PRIMARY_QUEUE_CONGESTION_DEPTH,
        "primary_max_buffer": PRIMARY_QUEUE_MAX_BUFFER,
        "recover_depth": PRIMARY_QUEUE_RECOVER_DEPTH,
        "stuck_seconds": PRIMARY_QUEUE_STUCK_SECONDS,
    }

def _safe_set_on_insert(default_doc: dict, set_doc: dict) -> dict:
    """
    MongoDB rejects updates that modify the same path in multiple operators
    (e.g., $set and $setOnInsert). This helper removes any overlapping keys.
    """
    try:
        blocked = set((set_doc or {}).keys())
    except Exception:
        blocked = set()
    return {k: v for k, v in (default_doc or {}).items() if k not in blocked}


def _get_or_init_queue_control(channel: str) -> dict:
    channel = (channel or "").strip().lower()
    doc = queue_controls_collection.find_one({"channel": channel}, {"_id": 0})
    if doc:
        # Migrate legacy field name "rate limit" → "rate_limit"
        if "rate_limit" not in doc and "rate limit" in doc:
            try:
                legacy_val = int(doc.get("rate limit") or 0)
            except Exception:
                legacy_val = 0
            queue_controls_collection.update_one(
                {"channel": channel},
                {"$set": {"rate_limit": legacy_val, "updated_at": datetime.utcnow()}, "$unset": {"rate limit": ""}},
            )
            doc["rate_limit"] = legacy_val
            doc.pop("rate limit", None)

        legacy_backup = _channel_to_backup_queue_name(channel)
        if doc.get("backup_queue") == legacy_backup and doc.get("active_queue") != legacy_backup:
            doc["backup_queue"] = None
        merged = {**_default_queue_control(channel), **doc}
        missing = {k: v for k, v in merged.items() if k not in doc}
        if missing:
            queue_controls_collection.update_one(
                {"channel": channel},
                {"$set": {**missing, "updated_at": datetime.utcnow()}},
            )
        return merged
    doc = {**_default_queue_control(channel), "updated_at": datetime.utcnow()}
    queue_controls_collection.update_one({"channel": channel}, {"$setOnInsert": doc}, upsert=True)
    doc.pop("updated_at", None)
    return _default_queue_control(channel)


def _purge_rabbitmq_queue(queue_name: str) -> int:
    """
    Purge messages from a specific RabbitMQ queue.
    Returns number of messages purged (best-effort).
    """
    broker_url = celery_app.conf.broker_url or "amqp://guest:guest@localhost:5672//"
    with Connection(broker_url) as conn:
        channel = conn.channel()
        try:
            result = channel.queue_purge(queue=queue_name)
            # kombu returns an integer on some versions, dict on others
            if isinstance(result, int):
                return result
            if isinstance(result, dict):
                return int(result.get("message_count", 0))
            return 0
        finally:
            channel.close()


def _rabbitmq_queue_depth(queue_name: str) -> int:
    """Get message count in a queue (passive declare)."""
    broker_url = celery_app.conf.broker_url or "amqp://guest:guest@localhost:5672//"
    with Connection(broker_url) as conn:
        channel = conn.channel()
        try:
            q = channel.queue_declare(queue=queue_name, passive=True)
            # q is (queue, message_count, consumer_count) in many kombu versions
            if isinstance(q, tuple) and len(q) >= 2:
                return int(q[1])
            # sometimes q is a dict-like
            if isinstance(q, dict):
                return int(q.get("message_count", 0))
            return 0
        finally:
            channel.close()


def _rabbitmq_queue_stats(queue_name: str) -> dict:
    """
    Best-effort queue stats from passive declare.
    Returns waiting message count and consumer count.
    """
    broker_url = celery_app.conf.broker_url or "amqp://guest:guest@localhost:5672//"
    with Connection(broker_url) as conn:
        channel = conn.channel()
        try:
            q = channel.queue_declare(queue=queue_name, passive=True)
            if isinstance(q, tuple):
                return {
                    "queue_name": queue_name,
                    "message_count": int(q[1]) if len(q) >= 2 else 0,
                    "consumer_count": int(q[2]) if len(q) >= 3 else 0,
                }
            if isinstance(q, dict):
                return {
                    "queue_name": queue_name,
                    "message_count": int(q.get("message_count", 0)),
                    "consumer_count": int(q.get("consumer_count", 0)),
                }
            return {"queue_name": queue_name, "message_count": 0, "consumer_count": 0}
        finally:
            channel.close()


def _declare_rabbitmq_queue(queue_name: str) -> None:
    broker_url = celery_app.conf.broker_url or "amqp://guest:guest@localhost:5672//"
    with Connection(broker_url) as conn:
        q = Queue(queue_name)
        q.maybe_bind(conn)
        q.declare()


def _delete_rabbitmq_queue(queue_name: str) -> None:
    broker_url = celery_app.conf.broker_url or "amqp://guest:guest@localhost:5672//"
    with Connection(broker_url) as conn:
        channel = conn.channel()
        try:
            channel.queue_delete(queue=queue_name)
        finally:
            channel.close()


def _attach_workers_to_queue(queue_name: str) -> None:
    try:
        celery_app.control.add_consumer(queue_name, reply=False)
    except Exception as exc:
        print(f"[QUEUE_FAILOVER] add_consumer failed for {queue_name}: {exc}")


def _detach_workers_from_queue(queue_name: str) -> None:
    try:
        celery_app.control.cancel_consumer(queue_name, reply=False)
    except Exception as exc:
        print(f"[QUEUE_FAILOVER] cancel_consumer failed for {queue_name}: {exc}")


def _new_dynamic_backup_queue_name(channel: str) -> str:
    channel = (channel or "").strip().lower() or "email"
    return f"{channel}-notify-q-backup-{uuid.uuid4().hex[:8]}"


def _serialize_job_for_celery(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_job_for_celery(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize_job_for_celery(v) for k, v in value.items() if k != "_id"}
    return value


def _parse_any_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
    return None


def _oldest_queued_job_age_seconds(channel: str, queue_name: str) -> int:
    doc = notification_jobs_collection.find_one(
        {"channel": channel, "queue_name": queue_name, "status": "QUEUED"},
        {"_id": 0, "created_at": 1, "updated_at": 1},
        sort=[("created_at", 1)],
    )
    if not doc:
        return 0
    ts = _parse_any_datetime(doc.get("updated_at")) or _parse_any_datetime(doc.get("created_at"))
    if not ts:
        return 0
    return max(0, int((datetime.utcnow() - ts).total_seconds()))


def _primary_queue_health(channel: str) -> dict:
    channel = (channel or "").strip().lower() or "email"
    control = _get_or_init_queue_control(channel)
    primary_queue = _channel_to_queue_name(channel)
    backup_queue = control.get("backup_queue")

    try:
        primary_stats = _rabbitmq_queue_stats(primary_queue)
        primary_error = None
    except Exception as exc:
        primary_stats = {"queue_name": primary_queue, "message_count": 0, "consumer_count": 0}
        primary_error = str(exc)

    if backup_queue:
        try:
            backup_stats = _rabbitmq_queue_stats(backup_queue)
            backup_error = None
        except Exception as exc:
            backup_stats = {"queue_name": backup_queue, "message_count": 0, "consumer_count": 0}
            backup_error = str(exc)
    else:
        backup_stats = {"queue_name": None, "message_count": 0, "consumer_count": 0}
        backup_error = None

    congestion_depth = int(control.get("congestion_depth", PRIMARY_QUEUE_CONGESTION_DEPTH))
    primary_max_buffer = int(control.get("primary_max_buffer", PRIMARY_QUEUE_MAX_BUFFER))
    recover_depth = int(control.get("recover_depth", PRIMARY_QUEUE_RECOVER_DEPTH))
    stuck_seconds = int(control.get("stuck_seconds", PRIMARY_QUEUE_STUCK_SECONDS))
    oldest_age = _oldest_queued_job_age_seconds(channel, primary_queue)

    reasons = []
    if primary_error:
        reasons.append("primary_unavailable")
    if primary_stats["message_count"] >= congestion_depth:
        reasons.append("primary_congested")
    if primary_stats["message_count"] >= primary_max_buffer:
        reasons.append("primary_buffer_full")
    if primary_stats["message_count"] > 0 and primary_stats["consumer_count"] <= 0:
        reasons.append("primary_no_consumers")
    if primary_stats["message_count"] > 0 and oldest_age >= stuck_seconds:
        reasons.append("primary_stuck")

    unhealthy = bool(reasons)
    recoverable = (
        not primary_error
        and primary_stats["consumer_count"] > 0
        and primary_stats["message_count"] <= recover_depth
        and oldest_age < stuck_seconds
    )

    return {
        "channel": channel,
        "control": control,
        "primary_queue": primary_queue,
        "backup_queue": backup_queue,
        "primary_stats": primary_stats,
        "backup_stats": backup_stats,
        "primary_error": primary_error,
        "backup_error": backup_error,
        "oldest_queued_age_seconds": oldest_age,
        "unhealthy": unhealthy,
        "recoverable": recoverable,
        "reasons": reasons,
    }


def _should_switch_queue(control: dict, target_queue: str) -> bool:
    current = control.get("active_queue") or _channel_to_queue_name(control.get("channel") or "email")
    if current == target_queue:
        return False
    last = _parse_any_datetime(control.get("last_failover_at"))
    if not last:
        return True
    return (datetime.utcnow() - last).total_seconds() >= QUEUE_FAILOVER_MIN_SWITCH_SECONDS


def _ensure_dynamic_backup_queue(channel: str) -> str:
    channel = (channel or "").strip().lower() or "email"
    control = _get_or_init_queue_control(channel)
    existing = control.get("backup_queue")
    if existing:
        return str(existing)

    backup_queue = _new_dynamic_backup_queue_name(channel)
    _declare_rabbitmq_queue(backup_queue)
    _attach_workers_to_queue(backup_queue)
    queue_controls_collection.update_one(
        {"channel": channel},
        {
            "$set": {
                "backup_queue": backup_queue,
                "updated_at": datetime.utcnow(),
            },
            "$setOnInsert": _safe_set_on_insert(_default_queue_control(channel), {"backup_queue": backup_queue, "updated_at": datetime.utcnow()}),
        },
        upsert=True,
    )
    print(f"[QUEUE_FAILOVER] Created dynamic backup queue for {channel}: {backup_queue}")
    return backup_queue


def _set_channel_active_queue(channel: str, target_queue: str, reason: Optional[str]) -> None:
    channel = (channel or "").strip().lower() or "email"
    queue_role = _queue_role(target_queue)
    queue_controls_collection.update_one(
        {"channel": channel},
        {
            "$set": {
                "active_queue": target_queue,
                "backup_queue": (target_queue if queue_role == "backup" else _get_or_init_queue_control(channel).get("backup_queue")),
                "backup_enabled": True,
                "last_failover_reason": reason,
                "last_failover_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
            "$setOnInsert": _safe_set_on_insert(
                _default_queue_control(channel),
                {
                    "active_queue": target_queue,
                    "backup_queue": (target_queue if queue_role == "backup" else _get_or_init_queue_control(channel).get("backup_queue")),
                    "backup_enabled": True,
                    "last_failover_reason": reason,
                    "last_failover_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                },
            ),
        },
        upsert=True,
    )


def _cleanup_dynamic_backup_queue(channel: str) -> None:
    channel = (channel or "").strip().lower() or "email"
    control = _get_or_init_queue_control(channel)
    backup_queue = control.get("backup_queue")
    if not backup_queue:
        return
    if backup_queue == _channel_to_queue_name(channel):
        return

    queued_jobs = notification_jobs_collection.count_documents(
        {"channel": channel, "queue_name": backup_queue, "status": "QUEUED"}
    )
    try:
        stats = _rabbitmq_queue_stats(backup_queue)
        broker_waiting = int(stats.get("message_count", 0))
    except Exception:
        broker_waiting = 0

    if queued_jobs > 0 or broker_waiting > 0:
        return

    _detach_workers_from_queue(backup_queue)
    try:
        _delete_rabbitmq_queue(backup_queue)
    except Exception as exc:
        print(f"[QUEUE_FAILOVER] Could not delete backup queue {backup_queue}: {exc}")
        return

    queue_controls_collection.update_one(
        {"channel": channel},
        {"$set": {"backup_queue": None, "updated_at": datetime.utcnow()}},
    )
    print(f"[QUEUE_FAILOVER] Removed dynamic backup queue for {channel}: {backup_queue}")


def _redirect_queued_jobs(channel: str, source_queue: str, target_queue: str, reason: str) -> dict:
    """
    Best-effort broker drain + republish for jobs still marked QUEUED.
    This moves only waiting jobs, not ones already processing.
    """
    docs = list(
        notification_jobs_collection
        .find(
            {"channel": channel, "queue_name": source_queue, "status": "QUEUED"},
            {"_id": 0},
        )
    )
    if not docs:
        return {"moved_jobs": 0, "purged_messages": 0}

    purged = 0
    try:
        purged = _purge_rabbitmq_queue(source_queue)
    except Exception:
        purged = 0

    moved = 0
    switched_at = datetime.utcnow()
    for doc in docs:
        updated = notification_jobs_collection.update_one(
            {"job_id": doc.get("job_id"), "status": "QUEUED", "queue_name": source_queue},
            {
                "$set": {
                    "queue_name": target_queue,
                    "queue_role": _queue_role(target_queue),
                    "updated_at": switched_at,
                    "failover_meta": {
                        "reason": reason,
                        "source_queue": source_queue,
                        "target_queue": target_queue,
                        "switched_at": switched_at,
                    },
                }
            },
        )
        if updated.modified_count <= 0:
            continue
        payload = _serialize_job_for_celery({**doc, "queue_name": target_queue, "queue_role": _queue_role(target_queue)})
        send_notification.apply_async(args=[payload], queue=target_queue)
        moved += 1

    return {"moved_jobs": moved, "purged_messages": purged}


def _reconcile_channel_failover(channel: str, *, drain_primary: bool) -> dict:
    health = _primary_queue_health(channel)
    control = health["control"]
    primary_queue = health["primary_queue"]
    backup_queue = health["backup_queue"]
    active_queue = control.get("active_queue") or primary_queue

    if health["unhealthy"] and control.get("backup_enabled", True):
        if not backup_queue:
            backup_queue = _ensure_dynamic_backup_queue(channel)
            health["backup_queue"] = backup_queue
        else:
            _attach_workers_to_queue(backup_queue)
        if _should_switch_queue(control, backup_queue):
            reason = ",".join(health["reasons"]) or "primary_unhealthy"
            _set_channel_active_queue(channel, backup_queue, reason)
            moved = {"moved_jobs": 0, "purged_messages": 0}
            if drain_primary:
                moved = _redirect_queued_jobs(channel, primary_queue, backup_queue, reason)
            health["active_queue"] = backup_queue
            health["failover_action"] = {"target_queue": backup_queue, **moved, "reason": reason}
            return health
        health["active_queue"] = active_queue
        return health

    if active_queue == backup_queue and health["recoverable"] and _should_switch_queue(control, primary_queue):
        _set_channel_active_queue(channel, primary_queue, "primary_recovered")
        health["active_queue"] = primary_queue
        health["failover_action"] = {"target_queue": primary_queue, "moved_jobs": 0, "purged_messages": 0, "reason": "primary_recovered"}
        _cleanup_dynamic_backup_queue(channel)
        return health

    health["active_queue"] = active_queue
    if active_queue == backup_queue and backup_queue:
        _attach_workers_to_queue(backup_queue)
    if active_queue == primary_queue:
        _cleanup_dynamic_backup_queue(channel)
    return health


def _select_queue_for_dispatch(channel: str) -> str:
    channel = (channel or "").strip().lower() or "email"
    if not QUEUE_FAILOVER_ENABLED:
        return _channel_to_queue_name(channel)
    health = _reconcile_channel_failover(channel, drain_primary=False)
    return health.get("active_queue") or _channel_to_queue_name(channel)


def run_queue_failover_monitor():
    if not QUEUE_FAILOVER_ENABLED:
        return
    print("[QUEUE_FAILOVER] Monitor started")
    while True:
        try:
            for channel in CHANNELS:
                _reconcile_channel_failover(channel, drain_primary=True)
        except Exception as exc:
            print(f"[QUEUE_FAILOVER] Monitor error (continuing): {exc}")
        time.sleep(QUEUE_FAILOVER_MONITOR_INTERVAL_SEC)


if QUEUE_FAILOVER_ENABLED:
    threading.Thread(target=run_queue_failover_monitor, daemon=True).start()


class QueueControlBody(BaseModel):
    queue: str


class QueueRateLimitBody(BaseModel):
    queue: str
    rate: int


# ── GLOBAL RATE LIMITING ──────────────────────────────────────────────────────
#
# Two layers of global protection, applied as FastAPI middleware BEFORE any
# route logic or per-user/per-client checks:
#
#   1. Per-IP sliding-window limit (in-memory)
#      Prevents a single IP from flooding the API — brute-force, scraping,
#      accidental tight loops. Uses a thread-safe defaultdict of (count, ts).
#
#   2. API-wide (system-level) rolling-window limit (in-memory)
#      Hard cap on total requests the entire service accepts in a time window.
#      Protects downstream providers and MongoDB from traffic surges.
#
# Both counters live in memory (reset on restart). For a multi-process
# deployment, move them to Redis — replace the dicts with redis.incr + expire.
#
# Configuration — tune without touching logic:

GLOBAL_RATE_LIMIT_CONFIG = {
    # Per IP address — applies to ALL endpoints
    "per_ip": {
        "max_requests":   5000,    # max requests per IP in the window
        "window_seconds": 60,     # rolling window length (1 minute)
    },
    # System-wide — applies across all IPs and clients combined
    "system": {
        "max_requests":   5000,   # max total requests to this API in the window
        "window_seconds": 60,     # rolling window length (1 minute)
    },
}

# ── In-memory sliding-window counters ─────────────────────────────────────────
# Structure: { key: {"count": int, "window_start": float (unix ts)} }
# Protected by a lock so concurrent Uvicorn threads don't race.

_rl_lock          = threading.Lock()
_ip_counters      = defaultdict(lambda: {"count": 0, "window_start": time.time()})
_system_counter   = {"count": 0, "window_start": time.time()}


def _check_global_rate_limit(ip: str) -> dict:
    """
    Checks both per-IP and system-wide sliding-window counters.

    Returns:
      {"allowed": True, "remaining_ip": N, "remaining_system": N}
      {"allowed": False, "scope": "ip"|"system", "retry_after": N,
       "limit": N, "window_seconds": N}

    Thread-safe — uses a single lock for both counters so the two checks
    are atomic relative to each other.
    """
    now = time.time()

    with _rl_lock:
        ip_cfg  = GLOBAL_RATE_LIMIT_CONFIG["per_ip"]
        sys_cfg = GLOBAL_RATE_LIMIT_CONFIG["system"]

        # ── Reset windows if expired ─────────────────────────────────────────
        ip_slot = _ip_counters[ip]
        if now - ip_slot["window_start"] >= ip_cfg["window_seconds"]:
            ip_slot["count"]        = 0
            ip_slot["window_start"] = now

        if now - _system_counter["window_start"] >= sys_cfg["window_seconds"]:
            _system_counter["count"]        = 0
            _system_counter["window_start"] = now

        # ── Check system-wide limit first (cheaper, shared counter) ──────────
        if _system_counter["count"] >= sys_cfg["max_requests"]:
            retry_after = int(
                sys_cfg["window_seconds"] - (now - _system_counter["window_start"])
            )
            return {
                "allowed":       False,
                "scope":         "system",
                "limit":         sys_cfg["max_requests"],
                "window_seconds": sys_cfg["window_seconds"],
                "retry_after":   max(1, retry_after),
            }

        # ── Check per-IP limit ────────────────────────────────────────────────
        if ip_slot["count"] >= ip_cfg["max_requests"]:
            retry_after = int(
                ip_cfg["window_seconds"] - (now - ip_slot["window_start"])
            )
            return {
                "allowed":       False,
                "scope":         "ip",
                "limit":         ip_cfg["max_requests"],
                "window_seconds": ip_cfg["window_seconds"],
                "retry_after":   max(1, retry_after),
            }

        # ── Increment both counters ───────────────────────────────────────────
        ip_slot["count"]      += 1
        _system_counter["count"] += 1

        remaining_ip     = ip_cfg["max_requests"]  - ip_slot["count"]
        remaining_system = sys_cfg["max_requests"] - _system_counter["count"]

    return {
        "allowed":          True,
        "remaining_ip":     remaining_ip,
        "remaining_system": remaining_system,
    }


def _log_global_rate_limit_hit(ip: str, scope: str, limit: int,
                                window_seconds: int, retry_after: int):
    """Persists a global rate limit hit to MongoDB for auditing."""
    try:
        global_rate_limit_logs_collection.insert_one({
            "log_id":         f"grl_{uuid.uuid4().hex[:12]}",
            "ip":             ip,
            "scope":          scope,          # "ip" or "system"
            "limit":          limit,
            "window_seconds": window_seconds,
            "retry_after":    retry_after,
            "blocked_at":     datetime.utcnow() + timedelta(hours=5, minutes=30),
        })
    except Exception:
        pass  # Never let logging failure block the response


@app.middleware("http")
async def global_rate_limit_middleware(request: Request, call_next):
    """
    FastAPI middleware that enforces global rate limits on every request.

    Runs BEFORE route handlers. On a hit:
      - Returns HTTP 429 with a JSON body and standard Retry-After header
      - Logs the hit to MongoDB asynchronously (fire-and-forget thread)
      - Skips the route handler entirely

    On a pass:
      - Injects X-RateLimit-Remaining-IP and X-RateLimit-Remaining-System
        headers into the response so callers can self-throttle.

    Excluded paths: /health, /docs, /openapi.json, /redoc
    (those should never count against limits)
    """
    excluded = {"/health", "/docs", "/openapi.json", "/redoc", "/favicon.ico"}
    if request.url.path in excluded:
        return await call_next(request)

    # Real IP — respect X-Forwarded-For when behind a proxy/load balancer
    forwarded_for = request.headers.get("X-Forwarded-For")
    ip = (forwarded_for.split(",")[0].strip()
          if forwarded_for else request.client.host)

    result = _check_global_rate_limit(ip)

    if not result["allowed"]:
        scope         = result["scope"]
        retry_after   = result["retry_after"]
        limit         = result["limit"]
        window        = result["window_seconds"]

        # Log asynchronously — don't block the 429 response on a DB write
        threading.Thread(
            target=_log_global_rate_limit_hit,
            args=(ip, scope, limit, window, retry_after),
            daemon=True,
        ).start()

        scope_msg = (
            f"Too many requests from your IP ({limit} per {window}s)."
            if scope == "ip"
            else f"API is temporarily at capacity ({limit} req/{window}s). Try again shortly."
        )
        print(f"[GLOBAL_RL] Blocked ip={ip} scope={scope} retry_after={retry_after}s")

        return Response(
            content=json.dumps({
                "detail":         scope_msg,
                "scope":          scope,
                "limit":          limit,
                "window_seconds": window,
                "retry_after_seconds": retry_after,
            }),
            status_code=429,
            headers={
                "Retry-After":    str(retry_after),
                "Content-Type":   "application/json",
                "X-RateLimit-Scope": scope,
            },
        )

    # Request allowed — call the actual route and inject info headers
    response = await call_next(request)
    response.headers["X-RateLimit-Remaining-IP"]     = str(result["remaining_ip"])
    response.headers["X-RateLimit-Remaining-System"] = str(result["remaining_system"])
    return response


@app.get("/global-rate-limit-logs")
def get_global_rate_limit_logs(
    ip:    Optional[str] = None,
    scope: Optional[str] = None,   # "ip" | "system"
    limit: int           = 50,
):
    """
    Query global rate limit hit logs (per-IP and system-wide blocks).

    Filters:
      ip    — show only hits from a specific IP address
      scope — "ip" (per-IP blocks) or "system" (API-wide capacity blocks)
      limit — max results (capped at 200), newest first
    """
    query: dict = {}
    if ip:    query["ip"]    = ip
    if scope: query["scope"] = scope

    limit = min(limit, 200)
    docs  = list(
        global_rate_limit_logs_collection
        .find(query, {"_id": 0})
        .sort("blocked_at", -1)
        .limit(limit)
    )
    return {"count": len(docs), "global_rate_limit_logs": docs}


@app.get("/rate-limit-status")
def get_rate_limit_status():
    """
    Returns the current state of the in-memory global rate limit counters.
    Useful for monitoring dashboards and debugging without reading logs.

    Shows: current count, window start, remaining capacity, and seconds
    left in the current window — for both per-IP aggregate and system-wide.

    Note: per-IP counters are shown as an aggregate summary (total unique IPs
    tracked), not individual IP counts, to avoid leaking IP-level details.
    """
    now = time.time()
    ip_cfg  = GLOBAL_RATE_LIMIT_CONFIG["per_ip"]
    sys_cfg = GLOBAL_RATE_LIMIT_CONFIG["system"]

    with _rl_lock:
        sys_elapsed    = now - _system_counter["window_start"]
        sys_remaining  = max(0, sys_cfg["window_seconds"] - sys_elapsed)
        unique_ips     = len(_ip_counters)

    return {
        "system": {
            "max_requests":       sys_cfg["max_requests"],
            "window_seconds":     sys_cfg["window_seconds"],
            "current_count":      _system_counter["count"],
            "remaining_capacity": max(0, sys_cfg["max_requests"] - _system_counter["count"]),
            "window_resets_in_seconds": round(sys_remaining, 1),
        },
        "per_ip": {
            "max_requests_per_ip": ip_cfg["max_requests"],
            "window_seconds":      ip_cfg["window_seconds"],
            "unique_ips_tracked":  unique_ips,
        },
        "config": GLOBAL_RATE_LIMIT_CONFIG,
    }


@app.get("/rate-limit/effective/{channel}")
def get_effective_rate_limit(channel: str, _: str = Depends(_require_bearer_token)):
    """
    Admin/debug endpoint: shows what the API will enforce for this channel
    given hardcoded defaults + provider caps + admin-configured jobs/min.
    """
    ch = _normalize_channel(channel)
    limits = RATE_LIMITS.get(ch) or {}
    per_user = (limits.get("per_user") or {})
    per_client = (limits.get("per_client") or {})

    provider_cap_sec = _provider_cap_jobs_per_sec(ch)
    provider_cap_min = max(0, int(provider_cap_sec * 60))
    admin_cap_min = _admin_rate_limit_jobs_per_min(ch)

    def _derived_max(window_seconds: int) -> int:
        if admin_cap_min <= 0:
            return 0
        effective_cap_min = min(admin_cap_min, provider_cap_min) if provider_cap_min > 0 else admin_cap_min
        return max(1, int(effective_cap_min * (float(window_seconds) / 60.0)))

    return {
        "channel": ch,
        "admin_rate_limit_jobs_per_min": admin_cap_min,
        "provider_cap_jobs_per_sec": provider_cap_sec,
        "provider_cap_jobs_per_min": provider_cap_min,
        "hardcoded": {
            "per_user": per_user,
            "per_client": per_client,
        },
        "effective": {
            "per_user_max": (_derived_max(int(per_user.get("window_seconds") or 60)) if admin_cap_min > 0 else int(per_user.get("max") or 0)),
            "per_user_window_seconds": int(per_user.get("window_seconds") or 0),
            "per_client_max": (_derived_max(int(per_client.get("window_seconds") or 3600)) if admin_cap_min > 0 else int(per_client.get("max") or 0)),
            "per_client_window_seconds": int(per_client.get("window_seconds") or 0),
        },
    }


# ── RATE LIMIT CONFIGURATION ──────────────────────────────────────────────────
# All windows are rolling (checked against now - window_seconds).

# Provider-side hard caps (jobs/sec) per channel.
# Admin-configured limits (via Queue Control UI) cannot exceed these.
# You can override these with env vars:
#   PROVIDER_CAP_EMAIL_JOBS_PER_SEC, PROVIDER_CAP_SMS_JOBS_PER_SEC,
#   PROVIDER_CAP_WHATSAPP_JOBS_PER_SEC, PROVIDER_CAP_PUSH_JOBS_PER_SEC
PROVIDER_RATE_CAPS_JOBS_PER_SEC = {
    "email":    int(os.getenv("PROVIDER_CAP_EMAIL_JOBS_PER_SEC", "100")),
    "sms":      int(os.getenv("PROVIDER_CAP_SMS_JOBS_PER_SEC", "20")),
    "whatsapp": int(os.getenv("PROVIDER_CAP_WHATSAPP_JOBS_PER_SEC", "10")),
    "push":     int(os.getenv("PROVIDER_CAP_PUSH_JOBS_PER_SEC", "200")),
}

RATE_LIMITS = {
    "email": {
        "per_user":   {"max": 5000,   "window_seconds": 60},    
        "per_client": {"max": 5000, "window_seconds": 60},   
    },
    "sms": {
        "per_user":   {"max": 5,    "window_seconds": 3600},
        "per_client": {"max": 500,  "window_seconds": 3600},
    },
    "whatsapp": {
        "per_user":   {"max": 5,    "window_seconds": 60},
        "per_client": {"max": 500,  "window_seconds": 3600},
    },
    "push": {
        "per_user":   {"max": 50,   "window_seconds": 3600},
        "per_client": {"max": 5000, "window_seconds": 3600},
    },
}

def _provider_cap_jobs_per_sec(channel: str) -> int:
    channel = (channel or "").strip().lower() or "email"
    cap = PROVIDER_RATE_CAPS_JOBS_PER_SEC.get(channel)
    if cap is None:
        cap = 50
    return max(0, int(cap))


def _admin_rate_limit_jobs_per_min(channel: str) -> int:
    """
    Returns admin-configured per-channel rate limit (jobs/min) from queue_controls.
    0 means "unset" (fall back to hardcoded RATE_LIMITS).
    """
    channel = (channel or "").strip().lower() or "email"
    doc = queue_controls_collection.find_one({"channel": channel}, {"_id": 0, "rate_limit": 1, "rate limit": 1})
    try:
        if doc and "rate_limit" not in doc and "rate limit" in doc:
            # best-effort migrate on read
            legacy_val = max(0, int(doc.get("rate limit", 0) or 0))
            queue_controls_collection.update_one(
                {"channel": channel},
                {"$set": {"rate_limit": legacy_val, "updated_at": datetime.utcnow()}, "$unset": {"rate limit": ""}},
            )
            return legacy_val
        return max(0, int((doc or {}).get("rate_limit", 0)))
    except Exception:
        return 0


# ── CLIENT AUTHENTICATION ────────────────────────────────────────────────────
#
# Flow:
#   1. Admin calls POST /clients/register to create a client and get an API key.
#   2. The raw API key is returned ONCE — only its SHA-256 hash is stored in DB.
#   3. Every call to /notify must include:
#        Header:  X-API-Key: <raw_api_key>
#        Body:    client_id must match the key owner
#   4. verify_api_key() dependency validates the key and returns the client doc.
#
# API key format:  nf_<32 random hex chars>
# Never stored in plain text — only SHA-256 hash is in MongoDB.


def _hash_key(raw_key: str) -> str:
    """SHA-256 hash of the raw API key. Only this is stored in DB."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _generate_api_key() -> str:
    """Generates a cryptographically secure API key: nf_<32 hex chars>"""
    return f"nf_{secrets.token_hex(32)}"


async def verify_api_key(
    x_api_key: str = Header(..., alias="X-API-Key"),
) -> dict:
    """
    FastAPI dependency — validates X-API-Key header on every protected route.
    Returns the client document if valid.
    Raises 401 if key is missing/invalid, 403 if client is inactive.
    """
    key_hash = _hash_key(x_api_key)
    client_doc = clients_collection.find_one({"api_key_hash": key_hash})

    if not client_doc:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key. Generate one via POST /clients/register.",
        )
    if not client_doc.get("is_active", True):
        raise HTTPException(
            status_code=403,
            detail=f"Client '{client_doc['client_id']}' is deactivated. Contact admin.",
        )
    # Attach derived metadata so downstream route handlers can enforce
    # token-level constraints (for example, event_type-bound API keys).
    client_doc["_verified_key_hash"] = key_hash
    return client_doc


async def verify_api_key_optional(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Optional[dict]:
    if not x_api_key:
        return None
    return await verify_api_key(x_api_key)


# ── MODELS ────────────────────────────────────────────────────────────────────

class Recipient(BaseModel):
    user_id:   str
    email:     Optional[str] = None
    phone:     Optional[str] = None
    wa_number: Optional[str] = None
    fcm_token: Optional[str] = None

class NotificationRequest(BaseModel):
    client_id:          str
    event_type:         str
    recipients:         list[Recipient]
    channels_requested: Optional[list[str]] = None
    priority:           Optional[str]       = None
    content:            Optional[dict]      = None

class EventChannelPref(BaseModel):
    event_type:  str
    channels:    list[str]
    dnd_mode:    Optional[str]        = "none"
    dnd_windows: Optional[list[dict]] = []

class DndWindow(BaseModel):
    days:  list[str]
    start: str
    end:   str
    tz:    str = "Asia/Kolkata"

class UserPreferencesRequest(BaseModel):
    # user_id can be passed in body OR via X-User-Id header.
    # Header takes priority. Body user_id kept for backward compatibility.
    # TODO: replace with JWT token decode when auth team delivers tokens.
    user_id:                   Optional[str]       = None
    email:                     Optional[str]       = None
    phone:                     Optional[str]       = None
    wa_number:                 Optional[str]       = None
    fcm_tokens:                Optional[list[str]] = None
    event_channel_preferences: Optional[list[EventChannelPref]] = None
    dnd_windows:               Optional[list[DndWindow]]        = None
    activity_pattern:          Optional[dict]                   = None


# ── DND CHECK ─────────────────────────────────────────────────────────────────

def is_in_dnd(dnd_windows: list) -> bool:
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    now_time = now.time()
    day_abbr = now.strftime("%a")
    for window in dnd_windows:
        days  = window.get("days", [])
        start = window.get("start")
        end   = window.get("end")
        tz    = window.get("tz", "Asia/Kolkata")
        if not start or not end:
            continue
        try:
            local_now = datetime.now(ZoneInfo(tz))
            now_time  = local_now.time()
            day_abbr  = local_now.strftime("%a")
        except Exception:
            pass
        if days and day_abbr not in days:
            continue
        start_t = datetime.strptime(start, "%H:%M").time()
        end_t   = datetime.strptime(end,   "%H:%M").time()
        if start_t > end_t:
            if now_time >= start_t or now_time <= end_t:
                return True
        else:
            if start_t <= now_time <= end_t:
                return True
    return False


# ── RATE LIMIT CHECK ──────────────────────────────────────────────────────────

def check_rate_limit(user_id: str, client_id: str, channel: str) -> dict:
    """
    Checks two rolling-window counters against notification_jobs:
      1. Per-user  limit: jobs sent to this user on this channel within window
      2. Per-client limit: jobs sent by this client on this channel within window

    Uses UTC datetimes throughout to match how MongoDB stores created_at.

    Returns:
      { "allowed": True }
      { "allowed": False, "scope": "user"|"client", "count": N,
        "limit": M, "retry_after_seconds": S, "channel": ch }
    """
    limits = RATE_LIMITS.get(channel)
    if not limits:
        return {"allowed": True}

    now_utc = datetime.utcnow()

    def _strip_tz(dt):
        """Make datetime naive (UTC) for safe arithmetic."""
        if dt is not None and hasattr(dt, "tzinfo") and dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt

    statuses = ["QUEUED", "PROCESSING", "SENT", "DELIVERED", "READ"]

    provider_cap_sec = _provider_cap_jobs_per_sec(channel)
    provider_cap_min = max(0, int(provider_cap_sec * 60))
    admin_cap_min = _admin_rate_limit_jobs_per_min(channel)

    # ── 1. Per-user check ────────────────────────────────────────────────────
    user_cfg    = limits["per_user"]
    user_window = timedelta(seconds=user_cfg["window_seconds"])
    user_since  = now_utc - user_window

    # If admin cap is set (>0), override hardcoded per-user max too.
    if admin_cap_min > 0:
        effective_cap_min = min(admin_cap_min, provider_cap_min) if provider_cap_min > 0 else admin_cap_min
        derived_user_max = max(1, int(effective_cap_min * (user_cfg["window_seconds"] / 60.0)))
        effective_user_max = derived_user_max
    else:
        effective_user_max = int(user_cfg["max"])

    # 1 job doc can contain many recipients; count per-recipient occurrences.
    user_count_docs = list(notification_jobs_collection.aggregate([
        {"$match": {
            "channel":    channel,
            "created_at": {"$gte": user_since},
            "status":     {"$in": statuses},
        }},
        {"$unwind": "$recipients"},
        {"$match": {"recipients.recipient.user_id": user_id}},
        {"$count": "n"},
    ]))
    user_count = int((user_count_docs[0]["n"] if user_count_docs else 0))

    if user_count >= effective_user_max:
        oldest = notification_jobs_collection.find_one(
            {
                "channel":           channel,
                "recipients.recipient.user_id": user_id,
                "created_at":        {"$gte": user_since},
            },
            sort=[("created_at", 1)],
        )
        retry_after = 0
        if oldest:
            oldest_created = _strip_tz(oldest["created_at"])
            expires_at     = oldest_created + user_window
            retry_after    = max(0, int((expires_at - now_utc).total_seconds()))

        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        _log_rate_limit_hit(user_id, client_id, channel, "user",
                            user_count, effective_user_max, now_ist)
        return {
            "allowed":             False,
            "scope":               "user",
            "count":               user_count,
            "limit":               effective_user_max,
            "window_seconds":      user_cfg["window_seconds"],
            "retry_after_seconds": retry_after,
            "channel":             channel,
        }

    # ── 2. Per-client check ──────────────────────────────────────────────────
    client_cfg    = limits["per_client"]
    client_window = timedelta(seconds=client_cfg["window_seconds"])
    client_since  = now_utc - client_window

    # Effective per-client cap:
    # - If admin cap is unset (0), fall back to hardcoded RATE_LIMITS.
    # - If admin cap is set (>0), it OVERRIDES hardcoded per_client max,
    #   and is capped only by provider capacity.
    if admin_cap_min <= 0:
        effective_client_max = int(client_cfg["max"])
    else:
        effective_cap_min = min(admin_cap_min, provider_cap_min) if provider_cap_min > 0 else admin_cap_min
        # Convert jobs/min → jobs/window
        derived_window_max = max(1, int(effective_cap_min * (client_cfg["window_seconds"] / 60.0)))
        effective_client_max = derived_window_max

    client_count_docs = list(notification_jobs_collection.aggregate([
        {"$match": {
            "channel":    channel,
            "client_id":  client_id,
            "created_at": {"$gte": client_since},
            "status":     {"$in": statuses},
        }},
        {"$project": {"n": {"$size": {"$ifNull": ["$recipients", []]}}}},
        {"$group": {"_id": None, "total": {"$sum": "$n"}}},
    ]))
    client_count = int((client_count_docs[0]["total"] if client_count_docs else 0))

    if client_count >= effective_client_max:
        oldest = notification_jobs_collection.find_one(
            {
                "channel":    channel,
                "client_id":  client_id,
                "created_at": {"$gte": client_since},
            },
            sort=[("created_at", 1)],
        )
        retry_after = 0
        if oldest:
            oldest_created = _strip_tz(oldest["created_at"])
            expires_at     = oldest_created + client_window
            retry_after    = max(0, int((expires_at - now_utc).total_seconds()))

        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        _log_rate_limit_hit(user_id, client_id, channel, "client",
                            client_count, effective_client_max, now_ist)
        return {
            "allowed":             False,
            "scope":               "client",
            "count":               client_count,
            "limit":               effective_client_max,
            "window_seconds":      client_cfg["window_seconds"],
            "retry_after_seconds": retry_after,
            "channel":             channel,
        }

    return {"allowed": True}


def _log_rate_limit_hit(user_id, client_id, channel, scope, count, limit, now_ist):
    """Writes a rate_limit_logs document for visibility / dashboards."""
    try:
        rate_limit_logs_collection.insert_one({
            "log_id":     f"rl_{uuid.uuid4().hex[:12]}",
            "user_id":    user_id,
            "client_id":  client_id,
            "channel":    channel,
            "scope":      scope,          # "user" or "client"
            "count":      count,
            "limit":      limit,
            "blocked_at": now_ist,
        })
    except Exception:
        pass  # Never let logging failure break the request path


# ── RESOLVE CHANNELS ──────────────────────────────────────────────────────────

def resolve_channels(user_id: str, event_type: str, channels_requested: list[str] | None) -> list[str]:
    """
    1. Look up user_preferences for this user_id
    2. Check global DND — if active, block all channels
    3. Find the event_channel_preferences entry for this event_type
    4. Check per-event DND if dnd_mode == 'per_event'
    5. Intersect user-allowed channels with caller-requested channels
    6. If no prefs found, fall back to channels_requested as-is
    """
    preference = preferences_collection.find_one({"user_id": user_id})
    if not preference:
        return channels_requested or []

    global_dnd = preference.get("dnd_windows", [])
    if global_dnd and is_in_dnd(global_dnd):
        return []

    event_prefs  = preference.get("event_channel_preferences", [])
    user_allowed = None
    for ep in event_prefs:
        if ep.get("event_type") == event_type:
            if ep.get("dnd_mode") == "per_event":
                if ep.get("dnd_windows") and is_in_dnd(ep["dnd_windows"]):
                    return []
            user_allowed = ep.get("channels", [])
            break

    if user_allowed is None:
        return channels_requested or []

    if not channels_requested:
        return user_allowed

    return [ch for ch in channels_requested if ch in user_allowed]


# ── CHANNEL → RECIPIENT ADDRESS MAPPING ──────────────────────────────────────

CHANNEL_ADDRESS_MAP = {
    "email":    lambda r: r.get("email"),
    "sms":      lambda r: r.get("phone"),
    "whatsapp": lambda r: r.get("wa_number"),
    "push":     lambda r: r.get("fcm_token"),
}


# ── CLIENT MANAGEMENT ROUTES ─────────────────────────────────────────────────

@app.post("/clients/register", status_code=201)
def register_client(body: dict):
    """
    Register a new API client and return their API key.

    Request body:
      {
        "name":          "Events Team",          (required)
        "monthly_quota": 100000,                 (optional, default 100000)
        "allowed_channels": ["email", "sms"]     (optional, default all channels)
      }

    Response:
      {
        "client_id": "evt_...",
        "api_key":   "nf_...",    ← shown ONCE, store it securely
        "name":      "Events Team",
        "monthly_quota": 100000
      }

    The raw API key is never stored — only its hash. If lost, revoke and re-register.
    """
    name = body.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required.")

    # Check for duplicate name
    if clients_collection.find_one({"name": name}):
        raise HTTPException(
            status_code=409,
            detail=f"A client named '{name}' already exists.",
        )

    raw_key       = _generate_api_key()
    client_id     = f"client_{uuid.uuid4().hex[:12]}"
    now_ist       = datetime.utcnow() + timedelta(hours=5, minutes=30)
    monthly_quota = body.get("monthly_quota", 10000000)
    allowed_channels = body.get("allowed_channels", ["email", "sms", "whatsapp", "push"])

    clients_collection.insert_one({
        "client_id":        client_id,
        "name":             name,
        "api_key_hash":     _hash_key(raw_key),
        "is_active":        True,
        "monthly_quota":    monthly_quota,
        "allowed_channels": allowed_channels,
        "created_at":       now_ist,
        "updated_at":       now_ist,
    })

    print(f"[AUTH] Registered client '{name}' → {client_id}")

    return {
        "client_id":        client_id,
        "api_key":          raw_key,      # returned ONCE — never stored in plain text
        "name":             name,
        "monthly_quota":    monthly_quota,
        "allowed_channels": allowed_channels,
        "message":          "Store this API key securely — it will not be shown again.",
    }


@app.post("/clients/sync")
def sync_client_from_node(body: dict):
    """
    Upsert a client record used by /notify, called from the Node login service.

    Node stores bcrypt hashes in Mongo `api_clients`; FastAPI only accepts keys
    checked against SHA-256 in `clients`. This endpoint registers the same
    plain `nf_...` key so Swagger /notify works with UI-generated keys.

    Body:
      client_id (str, required), plain_api_key (str, required),
      event_type (str, optional, default "DEFAULT"),
      name (str, optional), monthly_quota (int, optional),
      allowed_channels (list, optional)
    """
    client_id = (body.get("client_id") or "").strip()
    plain_api_key = body.get("plain_api_key")
    if not client_id or not plain_api_key or not isinstance(plain_api_key, str):
        raise HTTPException(
            status_code=400,
            detail="client_id and plain_api_key (string) are required.",
        )
    name = (body.get("name") or body.get("client_name") or "Client").strip()
    event_type = (body.get("event_type") or "DEFAULT").strip()
    if not event_type:
        event_type = "DEFAULT"
    monthly_quota = int(body.get("monthly_quota", 10000000))
    allowed_channels = body.get(
        "allowed_channels",
        ["email", "sms", "whatsapp", "push"],
    )
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    key_hash = _hash_key(plain_api_key.strip())

    existing = clients_collection.find_one({"client_id": client_id})
    base = {
        "client_id":        client_id,
        "name":             name,
        "api_key_hash":     key_hash,
        "api_key_event_type": event_type,
        "is_active":        True,
        "monthly_quota":    monthly_quota,
        "allowed_channels": allowed_channels,
        "updated_at":       now_ist,
    }
    if existing:
        clients_collection.update_one({"client_id": client_id}, {"$set": base})
    else:
        base["created_at"] = now_ist
        clients_collection.insert_one(base)

    print(f"[AUTH] Synced client '{client_id}' from Node → FastAPI clients collection")
    return {
        "message":   "Client API key synced for /notify.",
        "client_id": client_id,
    }


@app.get("/clients")
def list_clients():
    """List all registered clients (without exposing API key hashes)."""
    docs = list(clients_collection.find(
        {},
        {"_id": 0, "api_key_hash": 0}   # never expose the hash
    ).sort("created_at", -1))
    return {"count": len(docs), "clients": docs}


@app.patch("/clients/{client_id}")
def update_client(client_id: str, body: dict):
    """
    Update a client's settings.
    Allowed fields: is_active, monthly_quota, allowed_channels, name
    Use is_active=false to revoke access without deleting the client.
    """
    allowed_fields = {"is_active", "monthly_quota", "allowed_channels", "name"}
    update = {k: v for k, v in body.items() if k in allowed_fields}
    if not update:
        raise HTTPException(
            status_code=400,
            detail=f"No valid fields to update. Allowed: {allowed_fields}",
        )
    update["updated_at"] = datetime.utcnow() + timedelta(hours=5, minutes=30)
    result = clients_collection.update_one(
        {"client_id": client_id},
        {"$set": update},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")
    return {"message": "Client updated.", "client_id": client_id, "updated": update}


@app.post("/clients/{client_id}/rotate-key")
def rotate_api_key(client_id: str):
    """
    Revoke the current API key and issue a new one.
    Use this if a key is compromised or lost.
    The new key is returned ONCE — store it securely.
    """
    existing = clients_collection.find_one({"client_id": client_id})
    if not existing:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")

    new_raw_key = _generate_api_key()
    now_ist     = datetime.utcnow() + timedelta(hours=5, minutes=30)

    clients_collection.update_one(
        {"client_id": client_id},
        {"$set": {
            "api_key_hash": _hash_key(new_raw_key),
            "updated_at":   now_ist,
        }},
    )

    print(f"[AUTH] Rotated API key for client '{client_id}'")

    return {
        "client_id": client_id,
        "api_key":   new_raw_key,   # returned ONCE
        "message":   "API key rotated. Store this securely — it will not be shown again.",
    }


def _remove_media_files_for_query(query: dict) -> int:
    media_docs = list(media_files_collection.find(query, {"_id": 0, "stored_path": 1}))
    removed_media_files = 0
    for doc in media_docs:
        path = doc.get("stored_path")
        if not path:
            continue
        try:
            os.unlink(path)
            removed_media_files += 1
        except FileNotFoundError:
            continue
        except Exception:
            continue
    return removed_media_files


def _delete_client_scoped_data(client_id: str, *, delete_client_doc: bool = True) -> dict:
    removed_media_files = _remove_media_files_for_query({"client_id": client_id})
    deleted_counts = {
        "clients": clients_collection.delete_one({"client_id": client_id}).deleted_count if delete_client_doc else 0,
        "notification_requests": notification_requests_collection.delete_many({"client_id": client_id}).deleted_count,
        "notification_jobs": notification_jobs_collection.delete_many({"client_id": client_id}).deleted_count,
        "delivery_logs": delivery_logs_collection.delete_many({"client_id": client_id}).deleted_count,
        "rate_limit_logs": rate_limit_logs_collection.delete_many({"client_id": client_id}).deleted_count,
        "webhook_calls": webhook_calls_collection.delete_many({"client_id": client_id}).deleted_count,
        "preferences": preferences_collection.delete_many({"client_id": client_id}).deleted_count,
        "media_files": media_files_collection.delete_many({"client_id": client_id}).deleted_count,
        "dlq": dlq_collection.delete_many({"client_id": client_id}).deleted_count,
    }
    deleted_counts["media_files_removed_from_disk"] = removed_media_files
    return deleted_counts


@app.delete("/clients")
def delete_all_clients(_: str = Depends(_require_bearer_token)):
    """
    Admin-only destructive delete of every client account and related data.
    """
    client_ids = [
        doc["client_id"]
        for doc in clients_collection.find({}, {"_id": 0, "client_id": 1})
        if doc.get("client_id")
    ]
    if not client_ids:
        return {
            "message": "No client accounts to delete.",
            "deleted_client_ids": [],
            "deleted_counts": {},
        }

    removed_media_files = _remove_media_files_for_query({"client_id": {"$in": client_ids}})
    deleted_counts = {
        "clients": clients_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "notification_requests": notification_requests_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "notification_jobs": notification_jobs_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "delivery_logs": delivery_logs_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "rate_limit_logs": rate_limit_logs_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "webhook_calls": webhook_calls_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "preferences": preferences_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "media_files": media_files_collection.delete_many({"client_id": {"$in": client_ids}}).deleted_count,
        "dlq": dlq_collection.delete_many({"$or": [{"client_id": {"$in": client_ids}}, {"client_id": {"$exists": False}}]}).deleted_count,
    }
    deleted_counts["media_files_removed_from_disk"] = removed_media_files

    return {
        "message": f"Deleted {deleted_counts['clients']} client account(s).",
        "deleted_client_ids": client_ids,
        "deleted_counts": deleted_counts,
    }


@app.delete("/clients/{client_id}")
def delete_client(client_id: str, _: str = Depends(_require_bearer_token)):
    """
    Admin-only destructive delete of a client and its client-scoped data.
    Keeps the operation bounded to documents linked by client_id.
    """
    existing = clients_collection.find_one({"client_id": client_id}, {"_id": 0, "client_id": 1})
    if not existing:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")

    deleted_counts = _delete_client_scoped_data(client_id)

    return {
        "message": f"Client '{client_id}' deleted.",
        "client_id": client_id,
        "deleted_counts": deleted_counts,
    }


# ── CLIENT WEBHOOK URL CONFIG ────────────────────────────────────────────────

class WebhookUrlBody(BaseModel):
    webhook_url: Optional[str] = None


@app.get("/clients/{client_id}/webhook-url")
def get_client_webhook_url(client_id: str, _: str = Depends(_require_bearer_token)):
    doc = clients_collection.find_one({"client_id": client_id}, {"_id": 0, "client_id": 1, "webhook_url": 1})
    if not doc:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")
    return {"client_id": client_id, "webhook_url": doc.get("webhook_url")}


@app.put("/clients/{client_id}/webhook-url")
def set_client_webhook_url(client_id: str, body: WebhookUrlBody, _: str = Depends(_require_bearer_token)):
    url = (body.webhook_url or "").strip()
    update = {"updated_at": datetime.utcnow() + timedelta(hours=5, minutes=30)}
    if url:
        update["webhook_url"] = url
    else:
        update["webhook_url"] = None
    res = clients_collection.update_one({"client_id": client_id}, {"$set": update})
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")
    return {"message": "Webhook URL updated.", "client_id": client_id, "webhook_url": update["webhook_url"]}


# ── PREFERENCES ROUTES ────────────────────────────────────────────────────────
# user_id accepted two ways (header takes priority over body) for compatibility:
#   1. X-User-Id request header  ← teammate's approach, cleaner for API clients
#   2. user_id field in JSON body ← original approach, kept for backward compat
# TODO: replace both with JWT token decode when auth team delivers tokens.


@app.post("/preferences")
def set_preferences(
    pref: UserPreferencesRequest,
    x_user_id: Optional[str] = None,
):
    # Header takes priority; fall back to body field
    user_id = x_user_id or (pref.user_id.strip() if pref.user_id else None)
    if not user_id:
        raise HTTPException(
            status_code=400,
            detail="user_id is required. Send X-User-Id header or user_id in body."
        )
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    update_doc = {"user_id": user_id, "updated_at": now_ist}

    if pref.email            is not None: update_doc["email"]            = pref.email
    if pref.phone            is not None: update_doc["phone"]            = pref.phone
    if pref.wa_number        is not None: update_doc["wa_number"]        = pref.wa_number
    if pref.fcm_tokens       is not None: update_doc["fcm_tokens"]       = pref.fcm_tokens
    if pref.activity_pattern is not None: update_doc["activity_pattern"] = pref.activity_pattern

    if pref.event_channel_preferences is not None:
        update_doc["event_channel_preferences"] = [
            {
                "event_type":  ep.event_type,
                "channels":    ep.channels,
                "dnd_mode":    ep.dnd_mode,
                "dnd_windows": ep.dnd_windows or [],
            }
            for ep in pref.event_channel_preferences
        ]

    if pref.dnd_windows is not None:
        update_doc["dnd_windows"] = [
            {"days": dw.days, "start": dw.start, "end": dw.end, "tz": dw.tz}
            for dw in pref.dnd_windows
        ]

    is_new = preferences_collection.find_one({"user_id": user_id}) is None
    preferences_collection.update_one(
        {"user_id": user_id},
        {"$set": update_doc},
        upsert=True,
    )
    return {
        "message": "Preferences created." if is_new else "Preferences updated.",
        "user_id": user_id,
    }


@app.get("/preferences/{user_id}")
def get_preferences(user_id: str):
    pref = preferences_collection.find_one({"user_id": user_id}, {"_id": 0})
    if not pref:
        raise HTTPException(status_code=404, detail="No preferences found.")
    return pref


# ── PROVIDERS SNAPSHOT BUILDER ───────────────────────────────────────────────

# Hardcoded fallback used when MongoDB providers collection has no entries.
# Seed the providers collection to override these defaults.
_PROVIDER_DEFAULTS = {
    "email":    [{"provider_name": "SendGrid",    "provider_id": "sendgrid",    "priority": 1, "max_retries": 3, "timeout_ms": 5000},
                 {"provider_name": "SMTP",         "provider_id": "smtp",        "priority": 2, "max_retries": 2, "timeout_ms": 5000}],
    "sms":      [{"provider_name": "SendFire",    "provider_id": "sendfire",    "priority": 1, "max_retries": 3, "timeout_ms": 5000}],
    "whatsapp": [{"provider_name": "UltraMsg",    "provider_id": "ultramsg",    "priority": 1, "max_retries": 3, "timeout_ms": 5000}],
    "push":     [{"provider_name": "Firebase FCM","provider_id": "firebase_fcm","priority": 1, "max_retries": 3, "timeout_ms": 5000}],
}
_SUPPORTED_PROVIDER_NAMES = {
    name
    for providers in _PROVIDER_DEFAULTS.values()
    for name in [p.get("provider_name") for p in providers]
    if name
}

def _build_providers_snapshot(channel: str) -> list:
    """
    Returns an ordered list of providers to try for a given channel.
    Loads from MongoDB providers collection (dynamic) — falls back to
    hardcoded defaults if no active providers are found in the DB.
    tasks.py dispatch_with_failover() walks this list on failure.
    """
    db_providers = list(
        providers_collection.find(
            {"channel": channel, "is_active": True},
            {"_id": 0}
        ).sort("priority", 1)
    )

    valid_providers = [
        p for p in db_providers
        if p.get("provider_name") in _SUPPORTED_PROVIDER_NAMES
    ]

    if valid_providers:
        return [
            {
                "provider_id":   p.get("provider_id"),
                "provider_name": p.get("provider_name"),
                "priority":      p.get("priority"),
                "max_retries":   p.get("max_retries", 3),
                "timeout_ms":    p.get("timeout_ms", 5000),
            }
            for p in valid_providers
        ]

    # Fallback to hardcoded defaults
    return _PROVIDER_DEFAULTS.get(channel, [])


# ── CHARACTER LIMIT CONFIGURATION ────────────────────────────────────────────
#
# Limits are enforced at the API layer before jobs are queued.
# Based on provider-level constraints:
#   Email  subject : RFC 5321 hard limit is 998 chars (78 recommended)
#   Email  body    : No RFC hard limit — capped at 100k chars as sanity check
#   SMS    body    : 160 chars per GSM-7 segment. Long SMS auto-split into segments.
#                   Max 3 segments (480 chars) to avoid excessive charges.
#   WhatsApp body  : 4096 chars (UltraMsg platform limit)
#   Push   title   : 65 chars (FCM notification title limit)
#   Push   body    : 240 chars (FCM notification body limit)

CHANNEL_LIMITS = {
    "email": {
        "subject_max":      998,      # RFC 5321 hard limit
        "subject_warn":     78,       # RFC 5321 recommended max (soft)
        "body_max":         100_000,  # sanity cap on plain text body
    },
    "sms": {
        "body_segment_chars": 160,    # chars per GSM-7 SMS segment
        "max_segments":       3,      # reject if body would need more than 3 segments
    },
    "whatsapp": {
        "body_max":         4096,     # UltraMsg platform limit
    },
    "push": {
        "title_max":        65,       # FCM notification title
        "body_max":         240,      # FCM notification body
    },
}


def enforce_channel_limits(channel: str, content: dict) -> dict:
    """
    Validates and enforces per-channel character limits on content fields.

    Returns a result dict:
      {
        "valid":    True | False,
        "content":  <possibly modified content dict>,   # e.g. SMS split into segments
        "warnings": [...],   # soft limit warnings (logged, not rejected)
        "error":    str | None,   # set if valid=False
      }

    For SMS specifically:
      - Body <= 160 chars  → single segment, sent as-is
      - 160 < body <= 480  → split into segments list in content["sms_segments"]
      - Body > 480 chars   → rejected (would need 4+ segments, too expensive)
    """
    limits  = CHANNEL_LIMITS.get(channel, {})
    content = dict(content)   # work on a copy
    warnings = []

    # ── EMAIL ─────────────────────────────────────────────────────────────────
    if channel == "email":
        subject  = content.get("subject", "")
        body     = content.get("body") or content.get("message", "")

        # Subject hard limit
        if len(subject) > limits["subject_max"]:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"Email subject is {len(subject)} chars — exceeds hard limit of "
                    f"{limits['subject_max']} chars (RFC 5321). Please shorten the subject."
                ),
            }

        # Subject soft warning (doesn't reject, just logs)
        if len(subject) > limits["subject_warn"]:
            warnings.append(
                f"Email subject is {len(subject)} chars — recommended max is "
                f"{limits['subject_warn']} chars. Some clients may truncate it."
            )

        # Body hard limit
        if len(body) > limits["body_max"]:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"Email body is {len(body)} chars — exceeds max of "
                    f"{limits['body_max']:,} chars. Use a URL link for large content instead."
                ),
            }

    # ── SMS ───────────────────────────────────────────────────────────────────
    elif channel == "sms":
        body          = content.get("body") or content.get("message", "")
        seg_size      = limits["body_segment_chars"]   # 160
        max_segments  = limits["max_segments"]          # 3
        max_chars     = seg_size * max_segments          # 480

        if len(body) > max_chars:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"SMS body is {len(body)} chars — exceeds max of {max_chars} chars "
                    f"({max_segments} segments × {seg_size} chars). "
                    f"Shorten the message or use a URL to link to full content."
                ),
            }

        # Split into segments if body > 160 chars
        if len(body) > seg_size:
            segments = [body[i:i+seg_size] for i in range(0, len(body), seg_size)]
            content["sms_segments"] = segments
            warnings.append(
                f"SMS body ({len(body)} chars) split into {len(segments)} segments of "
                f"{seg_size} chars each. Each segment will be sent as a separate SMS."
            )
        else:
            content["sms_segments"] = [body]

    # ── WHATSAPP ──────────────────────────────────────────────────────────────
    elif channel == "whatsapp":
        body = content.get("body") or content.get("message", "")
        if len(body) > limits["body_max"]:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"WhatsApp message is {len(body)} chars — exceeds UltraMsg limit of "
                    f"{limits['body_max']} chars. Shorten the message or use a URL."
                ),
            }

    # ── PUSH ──────────────────────────────────────────────────────────────────
    elif channel == "push":
        title = content.get("title") or content.get("subject", "")
        body  = content.get("body", "")

        if len(title) > limits["title_max"]:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"Push notification title is {len(title)} chars — exceeds FCM limit of "
                    f"{limits['title_max']} chars. Shorten the title."
                ),
            }

        if len(body) > limits["body_max"]:
            return {
                "valid":    False,
                "content":  content,
                "warnings": warnings,
                "error":    (
                    f"Push notification body is {len(body)} chars — exceeds FCM limit of "
                    f"{limits['body_max']} chars. Shorten the body or use a URL."
                ),
            }

    return {"valid": True, "content": content, "warnings": warnings, "error": None}


# ── NOTIFICATION ROUTE ────────────────────────────────────────────────────────

@app.post("/notify")
def notify(
    http_request: Request,
    request:    NotificationRequest,
    api_client: dict = Depends(verify_api_key),
):
    """
    Flow:
      0. Authenticate client via X-API-Key header
      1. Store raw incoming request in notification_requests (no logic)
      2. For each recipient:
           a. resolve_channels() — DND check + user preference intersection
           b. For each resolved channel — create one notification_jobs doc
           c. Dispatch that job to the correct RabbitMQ queue via Celery
      3. Update notification_requests status
      4. Return summary
    """
    # ── Step 0: Validate client_id matches the API key owner ─────────────────
    if request.client_id != api_client["client_id"]:
        raise HTTPException(
            status_code=403,
            detail=(
                f"client_id '{request.client_id}' does not match the API key owner "
                f"'{api_client['client_id']}'. Use the client_id you received at registration."
            ),
        )

    # Enforce event-type scoping for event-specific API keys.
    # DEFAULT keys are treated as unscoped/backward-compatible keys.
    scoped_event_type_raw = api_client.get("api_key_event_type")
    scoped_event_type = scoped_event_type_raw.strip() if isinstance(scoped_event_type_raw, str) else ""
    if not scoped_event_type:
        raise HTTPException(
            status_code=403,
            detail=(
                "API key is missing event_type scope metadata. "
                "Regenerate/re-sync this key from the dashboard for the intended event_type."
            ),
        )
    if scoped_event_type != "DEFAULT" and request.event_type != scoped_event_type:
        raise HTTPException(
            status_code=403,
            detail=(
                f"API key is scoped to event_type '{scoped_event_type}', "
                f"but request used '{request.event_type}'."
            ),
        )

    # ── Check requested channels are allowed for this client ─────────────────
    if request.channels_requested:
        allowed = set(api_client.get("allowed_channels", ["email", "sms", "whatsapp", "push"]))
        blocked_channels = [ch for ch in request.channels_requested if ch not in allowed]
        if blocked_channels:
            raise HTTPException(
                status_code=403,
                detail=f"Channels {blocked_channels} are not allowed for this client.",
            )

    # ── Check monthly quota ───────────────────────────────────────────────────
    monthly_quota = api_client.get("monthly_quota", 10000000)
    month_start   = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_usage   = notification_jobs_collection.count_documents({
        "client_id":  request.client_id,
        "created_at": {"$gte": month_start},
        "status":     {"$nin": ["DROPPED"]},
    })
    if month_usage >= monthly_quota:
        # Calculate seconds until next month starts (UTC)
        now_utc_quota = datetime.utcnow()
        if now_utc_quota.month == 12:
            next_month = now_utc_quota.replace(year=now_utc_quota.year + 1, month=1, day=1,
                                               hour=0, minute=0, second=0, microsecond=0)
        else:
            next_month = now_utc_quota.replace(month=now_utc_quota.month + 1, day=1,
                                               hour=0, minute=0, second=0, microsecond=0)
        retry_after_quota = max(1, int((next_month - now_utc_quota).total_seconds()))
        raise HTTPException(
            status_code=429,
            detail=(
                f"Monthly quota of {monthly_quota} notifications exceeded "
                f"(used: {month_usage}). Resets at the start of next month. "
                f"Contact admin to increase quota."
            ),
            headers={"Retry-After": str(retry_after_quota)},
        )

    now_utc    = datetime.utcnow()          # used for notification_jobs (rate limit queries use UTC)
    now_ist    = now_utc + timedelta(hours=5, minutes=30)  # used for display-only collections
    request_id = f"req_{uuid.uuid4().hex[:12]}"

    notification_requests_collection.insert_one({
        "request_id":         request_id,
        "client_id":          request.client_id,
        "event_type":         request.event_type,
        "recipients":         [r.dict() for r in request.recipients],
        "channels_requested": request.channels_requested,
        "priority":           request.priority,
        "content":            request.content,
        "status":             "RECEIVED",
        "created_at":         now_ist,
    })

    jobs_created: list[str]  = []
    jobs_blocked: list[dict] = []
    channel_recipients: dict[str, list[dict]] = {}

    for recipient in request.recipients:
        recipient_dict = recipient.dict()

        channels_resolved = resolve_channels(
            recipient.user_id,
            request.event_type,
            request.channels_requested,
        )

        if not channels_resolved:
            jobs_blocked.append({
                "user_id": recipient.user_id,
                "reason":  "All channels blocked by DND or user preferences",
            })
            continue

        for channel in channels_resolved:
            rl = check_rate_limit(recipient.user_id, request.client_id, channel)
            if not rl["allowed"]:
                jobs_blocked.append({
                    "user_id":             recipient.user_id,
                    "channel":             channel,
                    "reason":              (
                        f"Rate limit exceeded ({rl['scope']} limit): "
                        f"{rl['count']}/{rl['limit']} per {rl['window_seconds']}s. "
                        f"Retry after {rl['retry_after_seconds']}s."
                    ),
                    "retry_after_seconds": rl["retry_after_seconds"],
                })
                print(
                    f"[RATE_LIMIT] Blocked {channel} for user={recipient.user_id} "
                    f"scope={rl['scope']} count={rl['count']}/{rl['limit']}"
                )
                continue

            # ── Character limit enforcement ──────────────────────────────
            normalized_content = _normalize_attachments(http_request, request.content or {})
            _validate_channel_attachments(channel, normalized_content)
            channel_content = {
                **normalized_content,
                "user_id":    recipient.user_id,
                "event_type": request.event_type,
            }
            limit_result = enforce_channel_limits(channel, channel_content)

            if not limit_result["valid"]:
                jobs_blocked.append({
                    "user_id": recipient.user_id,
                    "channel": channel,
                    "reason":  f"Content exceeds channel limit: {limit_result['error']}",
                })
                print(f"[CHAR_LIMIT] Blocked {channel} for user={recipient.user_id}: {limit_result['error']}")
                continue

            # Log soft warnings (subject too long, SMS split etc.)
            for w in limit_result["warnings"]:
                print(f"[CHAR_LIMIT] Warning ({channel}, user={recipient.user_id}): {w}")

            # Use the (possibly modified) content — e.g. SMS with sms_segments added
            validated_content = limit_result["content"]
            # ─────────────────────────────────────────────────────────────────

            recipient_address = CHANNEL_ADDRESS_MAP.get(channel, lambda r: None)(recipient_dict)
            if not recipient_address:
                required_field = {
                    "email": "email",
                    "sms": "phone",
                    "whatsapp": "wa_number",
                    "push": "fcm_token",
                }.get(channel, "recipient address")
                jobs_blocked.append({
                    "user_id": recipient.user_id,
                    "channel": channel,
                    "reason":  f"Missing {required_field} for {channel} notification.",
                })
                print(f"[RECIPIENT] Blocked {channel} for user={recipient.user_id}: missing {required_field}")
                continue

            queue_name = _select_queue_for_dispatch(channel)

            channel_recipients.setdefault(channel, []).append({
                "recipient": {
                    "user_id":   recipient.user_id,
                    "email":     recipient.email,
                    "phone":     recipient.phone,
                    "wa_number": recipient.wa_number,
                    "fcm_token": recipient.fcm_token,
                },
                "recipient_address": recipient_address,
                "content": validated_content,
                "retry_meta": {"attempt": 1, "provider_index": 0},
            })

    # ── Dispatch: 1 job per channel ──────────────────────────────────────────
    for channel, recipients_payload in channel_recipients.items():
        if not recipients_payload:
            continue

        job_id = f"job_{uuid.uuid4().hex[:12]}"
        queue_name = _select_queue_for_dispatch(channel)

        job_doc = {
            "job_id":           job_id,
            "request_id":       request_id,
            "client_id":        request.client_id,
            "event_type":       request.event_type,
            "channel":          channel,
            "priority":         request.priority or "MEDIUM",
            "template_id":      None,
            "rendered_content": None,
            "recipients":       recipients_payload,
            "retry_policy": {
                "max_attempts":    5,
                "current_attempt": 1,
                "backoff_seconds": [10, 30, 120, 600, 1800],
            },
            "queue_name": queue_name,
            "queue_role": _queue_role(queue_name),
            "providers_snapshot": _build_providers_snapshot(channel),
            "status":     "QUEUED",
            "created_at": now_utc,
            "updated_at": now_utc,
        }

        notification_jobs_collection.insert_one(job_doc)
        job_doc.pop("_id", None)
        job_doc["created_at"] = job_doc["created_at"].isoformat()
        job_doc["updated_at"] = job_doc["updated_at"].isoformat()

        send_notification.apply_async(args=[job_doc], queue=queue_name)

        jobs_created.append(job_id)

    for channel, recs in channel_recipients.items():
        print(f"[QUEUE] {channel} active_queue={_select_queue_for_dispatch(channel)} <- 1 job(s) for {len(recs)} recipient(s)")

    final_status = "JOBS_CREATED" if jobs_created else "FAILED"
    notification_requests_collection.update_one(
        {"request_id": request_id},
        {"$set": {"status": final_status}},
    )

    # ── If every job was blocked by rate limiting, return 429 with Retry-After ─
    if not jobs_created and jobs_blocked:
        rl_blocks = [b for b in jobs_blocked if b.get("retry_after_seconds")]
        if rl_blocks:
            # Retry-After = the longest wait across all blocked channels
            max_retry = max(b["retry_after_seconds"] for b in rl_blocks)
            raise HTTPException(
                status_code=429,
                detail={
                    "message":      "All notification jobs were rate-limited.",
                    "request_id":   request_id,
                    "jobs_created": [],
                    "jobs_blocked": jobs_blocked,
                },
                headers={"Retry-After": str(max_retry)},
            )

    return {
        "message":      "Notification request processed",
        "request_id":   request_id,
        "jobs_created": jobs_created,
        "jobs_blocked": jobs_blocked,
    }


# ── MEDIA UPLOAD + SERVE ──────────────────────────────────────────────────────
#
# Large docs/videos should be uploaded once, then referenced by file_id or URL in notify payload.
# This keeps /notify small and avoids base64 payload bloat.
#

@app.post("/media/upload")
async def upload_media(
    http_request: Request,
    file: Optional[UploadFile] = File(default=None),
    remote_url: Optional[str] = Form(default=None),
    name: Optional[str] = Form(default=None),
    mime_type: Optional[str] = Form(default=None),
    size_bytes: Optional[int] = Form(default=None),
    delivery_mode: Optional[str] = Form(default=None),
    api_client: dict = Depends(verify_api_key),
):
    if not file and not remote_url:
        raise HTTPException(status_code=400, detail="Provide either a file upload or remote_url.")
    if file and remote_url:
        raise HTTPException(status_code=400, detail="Provide only one of file or remote_url.")

    max_mb = int(os.getenv("MEDIA_UPLOAD_MAX_MB", "250"))
    max_bytes = max_mb * 1024 * 1024
    normalized_delivery_mode = (delivery_mode or "").strip().lower() or None
    if normalized_delivery_mode not in {None, "auto", "link_only", "provider_media"}:
        raise HTTPException(
            status_code=400,
            detail="delivery_mode must be one of: auto, link_only, provider_media.",
        )

    if remote_url:
        normalized_url = remote_url.strip()
        if not normalized_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="remote_url must start with http:// or https://")

        file_id = f"mf_{uuid.uuid4().hex[:12]}"
        resolved_name = (name or normalized_url.rstrip("/").split("/")[-1] or "remote-file").strip()
        resolved_mime = mime_type or _guess_mime(resolved_name)
        doc = {
            "file_id": file_id,
            "client_id": api_client["client_id"],
            "original_name": resolved_name,
            "mime_type": resolved_mime,
            "size_bytes": int(size_bytes) if isinstance(size_bytes, int) and size_bytes >= 0 else None,
            "source_url": normalized_url,
            "delivery_mode": normalized_delivery_mode or "link_only",
            "expires_at": datetime.utcnow() + timedelta(days=MEDIA_URL_TTL_DAYS),
            "created_at": datetime.utcnow(),
        }
        media_files_collection.insert_one(doc)
        return {
            "file_id": file_id,
            "url": normalized_url,
            "name": resolved_name,
            "mime_type": resolved_mime,
            "size_bytes": doc["size_bytes"],
            "delivery_mode": doc["delivery_mode"],
            "expires_at": doc["expires_at"],
            "storage": "remote_url",
        }

    original_name = file.filename or "upload.bin"
    file_id = f"mf_{uuid.uuid4().hex[:12]}"
    dest = MEDIA_DIR / file_id

    size = 0
    sha = hashlib.sha256()

    with dest.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                try:
                    f.close()
                    dest.unlink(missing_ok=True)
                except Exception:
                    pass
                raise HTTPException(status_code=413, detail=f"File too large. Max is {max_mb}MB.")
            sha.update(chunk)
            f.write(chunk)

    mime_type = file.content_type or _guess_mime(original_name)
    access_token = secrets.token_urlsafe(24)
    doc = {
        "file_id": file_id,
        "client_id": api_client["client_id"],
        "original_name": original_name,
        "mime_type": mime_type,
        "size_bytes": size,
        "sha256": sha.hexdigest(),
        "stored_path": str(dest),
        "access_token": access_token,
        "delivery_mode": normalized_delivery_mode or "auto",
        "expires_at": datetime.utcnow() + timedelta(days=MEDIA_URL_TTL_DAYS),
        "created_at": datetime.utcnow(),
    }
    media_files_collection.insert_one(doc)

    return {
        "file_id": file_id,
        # Tokenized URL so external providers (SendGrid/UltraMsg) can fetch.
        "url": _build_media_url(http_request, doc, include_token=True),
        "name": original_name,
        "mime_type": mime_type,
        "size_bytes": size,
        "delivery_mode": doc["delivery_mode"],
        "expires_at": doc["expires_at"],
        "storage": "local_file",
    }


@app.get("/media/{file_id}")
def get_media(
    file_id: str,
    token: Optional[str] = Query(default=None),
    api_client: Optional[dict] = Depends(verify_api_key_optional),
):
    doc = media_files_collection.find_one({"file_id": file_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="File not found")

    expires_at = doc.get("expires_at")
    is_expired = isinstance(expires_at, datetime) and expires_at <= datetime.utcnow()

    # Allow either:
    #  - authenticated access (dashboard / internal)
    #  - tokenized public access (for providers fetching media URLs)
    is_owner = bool(api_client and doc.get("client_id") == api_client.get("client_id"))
    if is_owner:
        allowed = True
    else:
        allowed = token and hmac.compare_digest(str(token), str(doc.get("access_token") or ""))
    if not allowed:
        raise HTTPException(status_code=403, detail="Not allowed")
    if not is_owner and is_expired:
        raise HTTPException(status_code=410, detail="Media URL expired")
    if is_owner and is_expired:
        doc = _refresh_media_doc_if_expired(doc)

    if doc.get("source_url"):
        return RedirectResponse(url=str(doc["source_url"]), status_code=307)
    path = doc.get("stored_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File missing on disk")
    return FileResponse(
        path,
        media_type=doc.get("mime_type") or "application/octet-stream",
        filename=doc.get("original_name") or file_id,
    )


@app.get("/jobs")
def get_jobs(
    client_id: Optional[str] = None,
    channel: Optional[str] = None,
    status: Optional[str] = None,
    event_type: Optional[str] = None,
    job_id: Optional[str] = None,
    limit: int = 200,
    skip: int = 0,
):
    """List notification jobs for the Job Explorer tab."""
    limit = min(limit, 500)
    query: dict = {}
    if client_id:
        query["client_id"] = client_id
    if channel:
        query["channel"] = channel
    if status:
        query["status"] = status
    if event_type:
        query["event_type"] = event_type
    if job_id:
        query["job_id"] = job_id

    total = notification_jobs_collection.count_documents(query)
    docs = list(
        notification_jobs_collection
        .find(
            query,
            {
                "_id": 0,
                "job_id": 1,
                "request_id": 1,
                "client_id": 1,
                "event_type": 1,
                "channel": 1,
                "status": 1,
                "created_at": 1,
                "updated_at": 1,
                "recipients.recipient.user_id": 1,
            },
        )
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    # Add derived fields for UI convenience
    job_ids = [d.get("job_id") for d in docs if d.get("job_id")]
    latest_outcomes: dict[str, dict[str, dict]] = defaultdict(dict)
    if job_ids:
        log_docs = list(
            delivery_logs_collection
            .find(
                {"job_id": {"$in": job_ids}},
                {
                    "_id": 0,
                    "job_id": 1,
                    "log_id": 1,
                    "recipient_user_id": 1,
                    "recipient_address": 1,
                    "status": 1,
                    "created_at": 1,
                    "error": 1,
                },
            )
            .sort("created_at", 1)
        )
        for log in log_docs:
            jid = log.get("job_id")
            if not jid:
                continue
            recipient_key = (
                log.get("recipient_user_id")
                or log.get("recipient_address")
                or f"log:{log.get('log_id')}"
            )
            latest_outcomes[jid][recipient_key] = log

    success_statuses = {"SENT", "DELIVERED", "READ"}
    failed_statuses = {"FAILED", "DLQ", "DROPPED", "SPAM"}
    for d in docs:
        recs = d.get("recipients") or []
        d["tasks"] = len(recs)
        try:
            d["user_ids"] = [r.get("recipient", {}).get("user_id") for r in recs if r.get("recipient", {}).get("user_id")]
        except Exception:
            d["user_ids"] = []
        outcomes = latest_outcomes.get(d.get("job_id"), {})
        succeeded_user_ids = []
        failed_user_ids = []
        latest_error = None
        latest_log_time = None
        latest_log_status = None
        latest_ms = -1.0
        for recipient_key, outcome in outcomes.items():
            status_text = str(outcome.get("status") or "").upper()
            user_id = outcome.get("recipient_user_id") or recipient_key
            if status_text in success_statuses:
                succeeded_user_ids.append(user_id)
            elif status_text in failed_statuses:
                failed_user_ids.append(user_id)
            created_at = outcome.get("created_at")
            try:
                created_ms = created_at.timestamp() if hasattr(created_at, "timestamp") else 0
            except Exception:
                created_ms = 0
            if created_ms >= latest_ms:
                latest_ms = created_ms
                latest_error = outcome.get("error")
                latest_log_time = created_at
                latest_log_status = outcome.get("status")

        if not outcomes and str(d.get("status") or "").upper() in failed_statuses:
            failed_user_ids = list(d.get("user_ids") or [])

        d["succeeded_tasks"] = len(succeeded_user_ids)
        d["failed_tasks"] = len(failed_user_ids)
        d["succeeded_user_ids"] = succeeded_user_ids
        d["failed_user_ids"] = failed_user_ids
        d["latest_log_status"] = latest_log_status
        d["latest_log_time"] = latest_log_time
        d["latest_error"] = latest_error
    return {"total": total, "skip": skip, "limit": limit, "count": len(docs), "jobs": docs}


# ── QUEUE ADMIN CONTROL ROUTES ────────────────────────────────────────────────

@app.get("/queue-stats")
def queue_stats(_: str = Depends(_require_bearer_token)):
    """
    Returns global + per-queue counts.
    - waiting: RabbitMQ depth (queued messages)
    - active:  Mongo jobs in PROCESSING
    - failed:  Mongo jobs in DLQ (and DROPPED)
    """
    queues = list(CHANNELS)

    # Mongo-side totals (fast aggregation by status)
    global_total = notification_jobs_collection.count_documents({})
    global_active = notification_jobs_collection.count_documents({"status": "PROCESSING"})
    global_failed = notification_jobs_collection.count_documents({"status": {"$in": ["DLQ", "DROPPED"]}})

    per_queue = []
    global_waiting = 0
    for ch in queues:
        control = _get_or_init_queue_control(ch)
        health = _primary_queue_health(ch) if QUEUE_FAILOVER_ENABLED else None
        qname = _channel_to_queue_name(ch)
        waiting = 0
        backup_waiting = 0
        active_queue = qname
        try:
            if health:
                waiting = int(health["primary_stats"]["message_count"])
                backup_waiting = int(health["backup_stats"]["message_count"])
                active_queue = health.get("active_queue") or active_queue
            else:
                waiting = _rabbitmq_queue_depth(qname)
        except Exception:
            waiting = 0

        active = notification_jobs_collection.count_documents({"channel": ch, "status": "PROCESSING"})
        failed = notification_jobs_collection.count_documents({"channel": ch, "status": {"$in": ["DLQ", "DROPPED"]}})

        global_waiting += waiting + backup_waiting
        per_queue.append({
            "type": ch.upper(),
            "channel": ch,
            "primary_queue": qname,
            "backup_queue": (health["backup_queue"] if health else None),
            "rate_limit": int((control or {}).get("rate_limit", 0) or 0),
            "waiting": waiting,
            "backup_waiting": backup_waiting,
            "active": active,
            "failed": failed,
            "active_queue": active_queue,
            "failover_reasons": (health.get("reasons") if health else []),
            "backup_enabled": bool((health or {}).get("control", {}).get("backup_enabled", control.get("backup_enabled", True))),
            "last_failover_reason": (health or {}).get("control", {}).get("last_failover_reason"),
            "last_failover_at": (
                ((health or {}).get("control", {}).get("last_failover_at").isoformat())
                if hasattr((health or {}).get("control", {}).get("last_failover_at"), "isoformat")
                else (health or {}).get("control", {}).get("last_failover_at")
            ),
        })

    return {
        "global": {
            "total": global_total,
            "waiting": global_waiting,
            "active": global_active,
            "failed": global_failed,
        },
        "queues": per_queue,
    }


@app.post("/pause-queue")
def pause_queue(body: QueueControlBody, _: str = Depends(_require_bearer_token)):
    channel = _normalize_channel(body.queue)
    queue_controls_collection.update_one(
        {"channel": channel},
        {
            "$set": {"paused": True, "updated_at": datetime.utcnow()},
            "$setOnInsert": _safe_set_on_insert(_default_queue_control(channel), {"paused": True, "updated_at": datetime.utcnow()}),
        },
        upsert=True,
    )
    return {"message": f"{channel.upper()} paused"}


@app.post("/resume-queue")
def resume_queue(body: QueueControlBody, _: str = Depends(_require_bearer_token)):
    channel = _normalize_channel(body.queue)
    queue_controls_collection.update_one(
        {"channel": channel},
        {
            "$set": {"paused": False, "updated_at": datetime.utcnow()},
            "$setOnInsert": _safe_set_on_insert(_default_queue_control(channel), {"paused": False, "updated_at": datetime.utcnow()}),
        },
        upsert=True,
    )
    return {"message": f"{channel.upper()} resumed"}


@app.post("/update-rate-limit")
def update_rate_limit(body: QueueRateLimitBody, _: str = Depends(_require_bearer_token)):
    channel = _normalize_channel(body.queue)
    rate = int(body.rate)
    if rate < 0:
        raise HTTPException(status_code=400, detail="rate must be >= 0")
    provider_cap_sec = _provider_cap_jobs_per_sec(channel)
    provider_cap_min = max(0, int(provider_cap_sec * 60))
    if provider_cap_min > 0 and rate > provider_cap_min:
        raise HTTPException(
            status_code=400,
            detail=f"rate must be <= provider cap ({provider_cap_min} jobs/min) for channel '{channel}'",
        )
    queue_controls_collection.update_one(
        {"channel": channel},
        {
            "$set": {"rate_limit": rate, "updated_at": datetime.utcnow()},
            "$unset": {"rate limit": ""},  # legacy cleanup if present
            "$setOnInsert": _safe_set_on_insert(_default_queue_control(channel), {"rate_limit": rate, "updated_at": datetime.utcnow()}),
        },
        upsert=True,
    )
    return {"message": f"{channel.upper()} rate limit updated to {rate} jobs/min"}


@app.post("/clear-queue")
def clear_queue(body: QueueControlBody, _: str = Depends(_require_bearer_token)):
    channel = _normalize_channel(body.queue)
    queue_name = _channel_to_queue_name(channel)
    purged = 0
    try:
        purged = _purge_rabbitmq_queue(queue_name)
    except Exception:
        purged = 0
    # Best-effort: mark queued jobs as CLEARED in Mongo so UI counts reconcile.
    # (Celery broker messages are authoritative for "waiting".)
    notification_jobs_collection.update_many(
        {"channel": channel, "status": "QUEUED"},
        {"$set": {"status": "CLEARED", "updated_at": datetime.utcnow()}},
    )
    return {"message": f"{channel.upper()} queue cleared", "tasks_removed": purged}


# ── USER EVENT TYPES ROUTE ────────────────────────────────────────────────────

@app.get("/user-event-types/{user_id}")
def get_user_event_types(user_id: str):
    """
    Returns distinct event_types from notification_requests
    where this user_id appears in the recipients array.
    Falls back to a default list if the user has no notification history yet.
    """
    pipeline = [
        {"$match":   {"recipients.user_id": user_id}},
        {"$group":   {"_id": "$event_type"}},
        {"$project": {"_id": 0, "event_type": "$_id"}},
    ]
    results     = list(notification_requests_collection.aggregate(pipeline))
    event_types = [r["event_type"] for r in results if r.get("event_type")]

    if not event_types:
        event_types = [
            "PAYMENT_SUCCESS", "OTP_LOGIN", "EXAM_ALERT",
            "ASSIGNMENT_DUE", "FEE_REMINDER", "PROMO", "SYSTEM_ALERT",
        ]

    return {"user_id": user_id, "event_types": event_types}


# ── DELIVERY LOGS QUERY ───────────────────────────────────────────────────────

@app.get("/logs")
def get_logs(
    client_id:  Optional[str] = None,
    user_id:    Optional[str] = None,
    event_type: Optional[str] = None,
    channel:    Optional[str] = None,
    status:     Optional[str] = None,   # SENT | DELIVERED | READ | FAILED | SPAM
    job_id:     Optional[str] = None,
    from_date:  Optional[str] = None,   # YYYY-MM-DD  (IST date)
    to_date:    Optional[str] = None,   # YYYY-MM-DD  (IST date)
    limit:      int           = 50,
    skip:       int           = 0,
):
    """
    Query delivery logs with optional filters. All parameters are optional.

    Filter params:
      client_id  — filter by the client who sent the notification
      user_id    — filter by recipient user
      event_type — e.g. PROMO, OTP_LOGIN, EXAM_ALERT
      channel    — email | sms | whatsapp | push
      status     — SENT | DELIVERED | READ | FAILED | SPAM
      from_date  — include logs created on or after this date (YYYY-MM-DD, IST)
      to_date    — include logs created on or before this date (YYYY-MM-DD, IST, inclusive)
      limit      — max results to return (capped at 500)
      skip       — offset for pagination

    Returns logs sorted by created_at descending (newest first).
    Each log includes job details joined from notification_jobs under a "job" key.
    """
    limit = min(limit, 500)

    date_filter: dict = {}
    if from_date or to_date:
        try:
            if from_date:
                # Start of day in IST (00:00:00)
                date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d")
            if to_date:
                # End of day in IST (23:59:59) — inclusive upper bound
                date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD (e.g. 2024-01-31)."
            )

    pipeline: list[dict] = []

    # Stage 1 — Join delivery_logs → notification_jobs on job_id
    pipeline.append({
        "$lookup": {
            "from":         "notification_jobs",
            "localField":   "job_id",
            "foreignField": "job_id",
            "as":           "_job_docs",
        }
    })

    # Stage 2 — Flatten the joined array (each log has exactly one job)
    pipeline.append({
        "$addFields": {
            "_job": {"$arrayElemAt": ["$_job_docs", 0]}
        }
    })

    # Stage 3 — Apply all filters in a single $match
    match: dict = {}

    if channel:    match["channel"]    = channel
    if status:     match["status"]     = status
    if event_type: match["event_type"] = event_type
    if job_id:     match["job_id"]     = job_id
    if date_filter:
        match["created_at"] = date_filter

    # user_id lives on delivery_logs (per send); client_id lives on the joined job document
    if user_id:   match["recipient_user_id"] = user_id
    if client_id: match["_job.client_id"]         = client_id

    if match:
        pipeline.append({"$match": match})

    # Stage 4 — Count total before pagination (separate pipeline for count)
    count_pipeline = pipeline + [{"$count": "total"}]
    count_result   = list(delivery_logs_collection.aggregate(count_pipeline))
    total          = count_result[0]["total"] if count_result else 0

    # Stage 5 — Sort, paginate
    pipeline.append({"$sort":  {"created_at": -1}})
    pipeline.append({"$skip":  skip})
    pipeline.append({"$limit": limit})

    # Stage 6 — Shape output.
    # Fix: MongoDB forbids mixing field inclusion and exclusion in the same
    # $project. Solution: use $project with explicit inclusions only, and
    # use $unset in a separate stage to remove the temporary _job/_job_docs fields.
    pipeline.append({
        "$project": {
            "_id":           0,
            "log_id":        1,
            "job_id":        1,
            "event_type":    1,
            "channel":       1,
            "provider":      1,
            "provider_message_id": 1,
            "status":        1,
            "attempt_number":1,
            "sent_at":       1,
            "delivered_at":  1,
            "read_at":       1,
            "latency_ms":    1,
            "error":         1,
            "created_at":    1,
            "recipient_user_id": 1,
            "recipient_address": 1,
            # Nest the enriched job fields under a "job" key
            "job": {
                "job_id":            "$_job.job_id",
                "client_id":         "$_job.client_id",
                "recipient_user_id": "$recipient_user_id",
                "recipient_address": "$recipient_address",
                "priority":          "$_job.priority",
                "provider_history":  "$_job.provider_history",
            },
        }
    })

    raw_logs = list(delivery_logs_collection.aggregate(pipeline))

    return {
        "total":  total,
        "skip":   skip,
        "limit":  limit,
        "count":  len(raw_logs),
        "logs":   raw_logs,
    }


@app.get("/stats")
def get_stats(
    client_id:  Optional[str] = None,
    event_type: Optional[str] = None,
    channel:    Optional[str] = None,
    from_date:  Optional[str] = None,
    to_date:    Optional[str] = None,
):
    """
    Aggregate delivery statistics.

    Returns per-channel breakdown of:
      - total sent
      - delivery rate  (DELIVERED + READ) / total × 100
      - failure rate   FAILED / total × 100
      - read rate      READ / total × 100
      - average latency in ms (for delivered messages)
      - spam count

    All filters are optional and work the same as GET /logs.
    """
    # ── Build base log query (fields that live directly on delivery_logs) ───────
    log_query: dict = {}
    if channel:    log_query["channel"]    = channel
    if event_type: log_query["event_type"] = event_type

    if from_date or to_date:
        date_filter: dict = {}
        try:
            if from_date:
                date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d")
            if to_date:
                date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD."
            )
        log_query["created_at"] = date_filter

    # ── Aggregation pipeline ─────────────────────────────────────────────────
    # Fix: client_id lives on notification_jobs, not delivery_logs.
    # We must $lookup first, then $match on the joined field AFTER the lookup.
    # The old code added the client_id filter to log_query before the lookup
    # ran, so it silently matched nothing.
    pipeline: list[dict] = []

    # Stage 1 — Apply filters on delivery_logs fields first (fast, uses indexes)
    if log_query:
        pipeline.append({"$match": log_query})

    # Stage 2 — Join to notification_jobs only when client_id filter is needed
    if client_id:
        pipeline.append({
            "$lookup": {
                "from":         "notification_jobs",
                "localField":   "job_id",
                "foreignField": "job_id",
                "as":           "_job_docs",
            }
        })
        pipeline.append({
            "$addFields": {"_job": {"$arrayElemAt": ["$_job_docs", 0]}}
        })
        # Now safe to filter on the joined field
        pipeline.append({"$match": {"_job.client_id": client_id}})

    pipeline += [
        {
            "$group": {
                "_id":          "$channel",
                "total":        {"$sum": 1},
                "sent":         {"$sum": {"$cond": [{"$eq": ["$status", "SENT"]},      1, 0]}},
                "delivered":    {"$sum": {"$cond": [{"$eq": ["$status", "DELIVERED"]}, 1, 0]}},
                "read":         {"$sum": {"$cond": [{"$eq": ["$status", "READ"]},      1, 0]}},
                "failed":       {"$sum": {"$cond": [{"$eq": ["$status", "FAILED"]},    1, 0]}},
                "spam":         {"$sum": {"$cond": [{"$eq": ["$status", "SPAM"]},      1, 0]}},
                "dlq":          {"$sum": {"$cond": [{"$eq": ["$status", "DLQ"]},       1, 0]}},
                "avg_latency_ms": {"$avg": "$latency_ms"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    raw = list(delivery_logs_collection.aggregate(pipeline))

    stats = []
    for row in raw:
        total     = row["total"] or 1
        delivered = row["delivered"] + row["read"]
        stats.append({
            "channel":           row["_id"],
            "total":             row["total"],
            "sent":              row["sent"],
            "delivered":         row["delivered"],
            "read":              row["read"],
            "failed":            row["failed"],
            "spam":              row["spam"],
            "dlq":               row["dlq"],
            "delivery_rate_pct": round(delivered / total * 100, 2),
            "read_rate_pct":     round(row["read"] / total * 100, 2),
            "failure_rate_pct":  round(row["failed"] / total * 100, 2),
            "avg_latency_ms":    round(row["avg_latency_ms"]) if row["avg_latency_ms"] else None,
        })

    if stats:
        grand_total     = sum(s["total"]                  for s in stats)
        grand_delivered = sum(s["delivered"] + s["read"]  for s in stats)
        grand_failed    = sum(s["failed"]                 for s in stats)
        grand_read      = sum(s["read"]                   for s in stats)
        latencies       = [s["avg_latency_ms"] for s in stats if s["avg_latency_ms"]]
        grand_latency   = round(sum(latencies) / len(latencies)) if latencies else None
        gt              = grand_total or 1
        summary = {
            "channel":           "ALL",
            "total":             grand_total,
            "delivery_rate_pct": round(grand_delivered / gt * 100, 2),
            "read_rate_pct":     round(grand_read      / gt * 100, 2),
            "failure_rate_pct":  round(grand_failed    / gt * 100, 2),
            "avg_latency_ms":    grand_latency,
        }
    else:
        summary = None

    return {
        "filters_applied": {
            k: v for k, v in {
                "client_id":  client_id,
                "event_type": event_type,
                "channel":    channel,
                "from_date":  from_date,
                "to_date":    to_date,
            }.items() if v
        },
        "summary":     summary,
        "per_channel": stats,
    }


@app.get("/latency")
def get_latency_dashboard(
    client_id:  Optional[str] = None,
    event_type: Optional[str] = None,
    channel:    Optional[str] = None,
    from_date:  Optional[str] = None,
    to_date:    Optional[str] = None,
    sample_limit: int         = 5000,
    _: str = Depends(_require_bearer_token),
):
    """
    Latency dashboard for delivered notifications.

    Latency definition:
      latency_ms = delivered_at - sent_at (computed when provider webhooks arrive).

    Returns:
      - summary (count, avg, p50, p90, p99, min, max)
      - per_channel breakdown with same metrics
      - daily trend (avg + p90) by day for quick charts
    """
    sample_limit = min(max(int(sample_limit or 5000), 200), 20000)

    # Base filters on delivery_logs fields
    log_query: dict = {"latency_ms": {"$ne": None}}
    if channel:
        log_query["channel"] = channel
    if event_type:
        log_query["event_type"] = event_type
    # Delivered-like statuses only
    log_query["status"] = {"$in": ["DELIVERED", "READ"]}

    if from_date or to_date:
        date_filter: dict = {}
        try:
            if from_date:
                date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d")
            if to_date:
                date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        log_query["created_at"] = date_filter

    # We need client_id filter via join to notification_jobs
    pipeline: list[dict] = [{"$match": log_query}]
    if client_id:
        pipeline += [
            {
                "$lookup": {
                    "from": "notification_jobs",
                    "localField": "job_id",
                    "foreignField": "job_id",
                    "as": "_job_docs",
                }
            },
            {"$addFields": {"_job": {"$arrayElemAt": ["$_job_docs", 0]}}},
            {"$match": {"_job.client_id": client_id}},
        ]

    # Grab a sample of latency rows for percentile calculations
    pipeline_sample = pipeline + [
        {"$project": {"_id": 0, "channel": 1, "latency_ms": 1, "created_at": 1}},
        {"$sort": {"created_at": -1}},
        {"$limit": sample_limit},
    ]
    rows = list(delivery_logs_collection.aggregate(pipeline_sample))

    def compute_metrics(values: list[int]) -> dict:
        if not values:
            return {"count": 0, "avg_ms": None, "p50_ms": None, "p90_ms": None, "p99_ms": None, "min_ms": None, "max_ms": None}
        vals = sorted(int(v) for v in values if v is not None)
        if not vals:
            return {"count": 0, "avg_ms": None, "p50_ms": None, "p90_ms": None, "p99_ms": None, "min_ms": None, "max_ms": None}
        avg = round(sum(vals) / len(vals))
        return {
            "count": len(vals),
            "avg_ms": avg,
            "p50_ms": _percentile(vals, 50),
            "p90_ms": _percentile(vals, 90),
            "p99_ms": _percentile(vals, 99),
            "min_ms": vals[0],
            "max_ms": vals[-1],
        }

    all_vals = [r.get("latency_ms") for r in rows if r.get("latency_ms") is not None]
    summary = compute_metrics(all_vals)

    per_channel_map: dict[str, list[int]] = defaultdict(list)
    per_day_map: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        v = r.get("latency_ms")
        if v is None:
            continue
        ch = (r.get("channel") or "unknown").lower()
        per_channel_map[ch].append(int(v))
        dt = r.get("created_at")
        day_key = None
        if hasattr(dt, "strftime"):
            day_key = dt.strftime("%Y-%m-%d")
        if day_key:
            per_day_map[day_key].append(int(v))

    per_channel = []
    for ch, vals in sorted(per_channel_map.items(), key=lambda kv: kv[0]):
        per_channel.append({"channel": ch, **compute_metrics(vals)})

    trend = []
    for day, vals in sorted(per_day_map.items(), key=lambda kv: kv[0]):
        m = compute_metrics(vals)
        trend.append({"day": day, "count": m["count"], "avg_ms": m["avg_ms"], "p90_ms": m["p90_ms"]})

    return {
        "filters_applied": {
            k: v for k, v in {
                "client_id": client_id,
                "event_type": event_type,
                "channel": channel,
                "from_date": from_date,
                "to_date": to_date,
                "sample_limit": sample_limit,
            }.items() if v
        },
        "summary": summary,
        "per_channel": per_channel,
        "trend": trend,
        "note": "Percentiles are computed from a recent sample (sorted by created_at desc).",
    }


@app.get("/rate-limit-logs")
def get_rate_limit_logs(
    user_id:   Optional[str] = None,
    client_id: Optional[str] = None,
    channel:   Optional[str] = None,
    scope:     Optional[str] = None,   # "user" | "client" | "global_ip" | "global_system"
    limit:     int           = 50,
):
    """
    Query rate limit hit logs. Covers all three layers:
      - scope=user         — per-user per-channel blocks (from rate_limit_logs)
      - scope=client       — per-client per-channel blocks (from rate_limit_logs)
      - scope=global_ip    — per-IP global blocks (from global_rate_limit_logs)
      - scope=global_system — system-wide capacity blocks (from global_rate_limit_logs)
      - scope omitted      — returns all of the above merged, newest first

    All filters are optional and can be combined.
    Returns most recent hits first, capped at `limit` (max 200).
    """
    limit = min(limit, 200)

    is_global_scope = scope in ("global_ip", "global_system")
    is_local_scope  = scope in ("user", "client")

    # ── Local (per-user / per-client) logs ────────────────────────────────────
    local_docs = []
    if not is_global_scope:
        local_query: dict = {}
        if user_id:                      local_query["user_id"]   = user_id
        if client_id:                    local_query["client_id"] = client_id
        if channel:                      local_query["channel"]   = channel
        if scope and not is_global_scope: local_query["scope"]    = scope
        local_docs = list(
            rate_limit_logs_collection
            .find(local_query, {"_id": 0})
            .sort("blocked_at", -1)
            .limit(limit)
        )
        for d in local_docs:
            d.setdefault("layer", "per_user_client")

    # ── Global (per-IP / system) logs ─────────────────────────────────────────
    global_docs = []
    if not is_local_scope:
        global_query: dict = {}
        if scope == "global_ip":     global_query["scope"] = "ip"
        if scope == "global_system": global_query["scope"] = "system"
        global_docs = list(
            global_rate_limit_logs_collection
            .find(global_query, {"_id": 0})
            .sort("blocked_at", -1)
            .limit(limit)
        )
        for d in global_docs:
            d.setdefault("layer", "global")

    # ── Merge and sort by blocked_at ──────────────────────────────────────────
    merged = local_docs + global_docs
    merged.sort(key=lambda d: d.get("blocked_at", datetime.min), reverse=True)
    merged = merged[:limit]

    return {
        "count":           len(merged),
        "scope_filter":    scope or "all",
        "rate_limit_logs": merged,
    }


# ── DLQ QUERY ENDPOINT ────────────────────────────────────────────────────────

@app.get("/dlq")
def get_dlq(
    status:     Optional[str] = None,   # DLQ | PROCESSING | REQUEUED | DROPPED
    channel:    Optional[str] = None,
    error_code: Optional[str] = None,
    limit:      int           = 50,
    skip:       int           = 0,
):
    """
    Query the Dead Letter Queue.
    Returns jobs that failed all retries, grouped by status.
    """
    query: dict = {}
    if status:     query["status"]     = status
    if channel:    query["channel"]    = channel
    if error_code: query["error_code"] = error_code

    limit = min(limit, 200)
    total = dlq_collection.count_documents(query)
    docs  = list(
        dlq_collection
        .find(query, {"_id": 0})
        .sort("failed_at", -1)
        .skip(skip)
        .limit(limit)
    )
    # Backfill queue/channel from notification_jobs for old DLQ rows that were
    # created before queue metadata was stored directly in DLQ documents.
    for d in docs:
        if d.get("queue_name") and d.get("channel"):
            continue
        original = notification_jobs_collection.find_one(
            {"job_id": d.get("job_id")},
            {"_id": 0, "queue_name": 1, "channel": 1},
        )
        if original:
            d["queue_name"] = d.get("queue_name") or original.get("queue_name")
            d["channel"] = d.get("channel") or original.get("channel")
    return {"total": total, "skip": skip, "limit": limit, "count": len(docs), "dlq": docs}


# ── DLQ ACTIONS (retry / discard / bulk operations) ───────────────────────────

class JobActionBody(BaseModel):
    job_id: str


class DiscardDlqBody(BaseModel):
    channel: Optional[str] = None


def _requeue_job_from_dlq(job_id: str, *, countdown: int = 0, switch_provider: bool = False) -> dict:
    """
    Reconstructs a Celery job payload from notification_jobs + dlq entry and requeues it.
    Returns metadata for UI.
    """
    original = notification_jobs_collection.find_one({"job_id": job_id}, {"_id": 0})
    if not original:
        raise HTTPException(status_code=404, detail=f"Original job not found: {job_id}")

    dlq_doc = dlq_collection.find_one({"job_id": job_id}, {"_id": 0}) or {}

    channel = original.get("channel") or dlq_doc.get("channel") or "email"
    queue_name = _select_queue_for_dispatch(channel)

    providers_snapshot = (
        original.get("providers_snapshot")
        or dlq_doc.get("providers_snapshot")
        or _build_providers_snapshot(channel)
    )
    if not providers_snapshot:
        raise HTTPException(status_code=400, detail="providers_snapshot missing — cannot requeue")

    recipient_dict = original.get("recipient") or dlq_doc.get("recipient") or {}
    recipient_address = original.get("recipient_address")
    if not recipient_address:
        # Fallback mapping (kept in sync with dlq_processor.py)
        if channel == "email":
            recipient_address = recipient_dict.get("email")
        elif channel == "sms":
            recipient_address = recipient_dict.get("phone")
        elif channel == "whatsapp":
            recipient_address = recipient_dict.get("wa_number")
        elif channel == "push":
            recipient_address = recipient_dict.get("fcm_token")

    if not recipient_address:
        raise HTTPException(status_code=400, detail=f"No recipient address for channel '{channel}'")

    current_index = int((original.get("retry_meta") or {}).get("provider_index", 0))
    if switch_provider:
        current_index = min(current_index + 1, max(0, len(providers_snapshot) - 1))

    new_job = {
        "job_id":            job_id,
        "request_id":        original.get("request_id"),
        "client_id":         original.get("client_id"),
        "event_type":        original.get("event_type"),
        "channel":           channel,
        "queue_name":        queue_name,
        "recipient":         recipient_dict,
        "recipient_address": recipient_address,
        "content":           dlq_doc.get("payload") or original.get("content", {}),
        "providers_snapshot": providers_snapshot,
        "retry_meta": {
            "attempt": 1,
            "provider_index": current_index,
        },
        "provider_history":  dlq_doc.get("providers_tried") or original.get("provider_history") or [],
        "created_at":        (original.get("created_at") or datetime.utcnow()).isoformat()
                             if hasattr((original.get("created_at") or datetime.utcnow()), "isoformat")
                             else original.get("created_at"),
    }

    send_notification.apply_async(args=[new_job], queue=queue_name, countdown=max(int(countdown or 0), 0))

    # Mark both DLQ + job as queued again
    dlq_collection.update_one(
        {"job_id": job_id},
        {"$set": {
            "status": "REQUEUED",
            "requeued_at": datetime.utcnow(),
        }, "$inc": {"requeue_count": 1}},
        upsert=True,
    )
    notification_jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {
            "status": "QUEUED",
            "queue_name": queue_name,
            "queue_role": _queue_role(queue_name),
            "retry_meta": new_job["retry_meta"],
            "updated_at": datetime.utcnow(),
        }},
    )

    return {"job_id": job_id, "channel": channel, "queue_name": queue_name, "countdown": max(int(countdown or 0), 0)}


@app.post("/retry-job")
def retry_job(body: JobActionBody, _: str = Depends(_require_bearer_token)):
    meta = _requeue_job_from_dlq(body.job_id, countdown=0)
    return {"message": f"Job {body.job_id} requeued", **meta}


@app.post("/discard-job")
def discard_job(body: JobActionBody, _: str = Depends(_require_bearer_token)):
    job_id = body.job_id
    dlq_collection.update_one(
        {"job_id": job_id},
        {"$set": {"status": "DROPPED", "dropped_at": datetime.utcnow(), "drop_reason": "Discarded from dashboard"}},
        upsert=False,
    )
    notification_jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {"status": "DROPPED", "updated_at": datetime.utcnow()}},
    )
    return {"message": f"Job {job_id} discarded"}


@app.post("/discard-dlq")
def discard_dlq(body: DiscardDlqBody, _: str = Depends(_require_bearer_token)):
    query: dict = {"status": "DLQ"}
    channel = None
    if body.channel:
        channel = _normalize_channel(body.channel)
        queue_name = _channel_to_queue_name(channel)
        backup_queue_name = _channel_to_backup_queue_name(channel)
        query["$or"] = [
            {"channel": channel},
            {"queue_name": {"$in": [queue_name, backup_queue_name]}},
        ]

    docs = list(dlq_collection.find(query, {"_id": 0, "job_id": 1}))
    job_ids = [d.get("job_id") for d in docs if d.get("job_id")]
    now = datetime.utcnow()

    dlq_result = dlq_collection.update_many(
        query,
        {"$set": {"status": "DROPPED", "dropped_at": now, "drop_reason": "Bulk discarded from dashboard"}},
    )
    job_result = notification_jobs_collection.update_many(
        {"job_id": {"$in": job_ids}},
        {"$set": {"status": "DROPPED", "updated_at": now}},
    ) if job_ids else None

    scope = f" for {channel.upper()}" if channel else ""
    return {
        "message": f"Discarded {dlq_result.modified_count} DLQ job(s){scope}.",
        "channel": channel,
        "matched": dlq_result.matched_count,
        "discarded": dlq_result.modified_count,
        "jobs_updated": job_result.modified_count if job_result else 0,
    }


@app.post("/retry-failed")
def retry_failed(_: str = Depends(_require_bearer_token), limit: int = 200):
    """
    Bulk retry for *transient* DLQ entries.
    """
    limit = min(max(int(limit), 1), 500)
    docs = list(
        dlq_collection.find(
            {"status": "DLQ", "error_type": "TRANSIENT"},
            {"_id": 0, "job_id": 1},
        ).limit(limit)
    )
    requeued = []
    failed = []
    for d in docs:
        jid = d.get("job_id")
        if not jid:
            continue
        try:
            requeued.append(_requeue_job_from_dlq(jid, countdown=0))
        except Exception as e:
            failed.append({"job_id": jid, "error": str(e)})
    return {"message": "Retry failed processed", "attempted": len(docs), "requeued": len(requeued), "failures": failed}


@app.post("/reprocess-dlq")
def reprocess_dlq(_: str = Depends(_require_bearer_token), limit: int = 200):
    """
    Bulk reprocess DLQ entries (skips PERMANENT).
    Respects next_retry_at if present by computing a countdown.
    """
    limit = min(max(int(limit), 1), 500)
    docs = list(
        dlq_collection.find(
            {"status": "DLQ", "error_type": {"$ne": "PERMANENT"}},
            {"_id": 0, "job_id": 1, "next_retry_at": 1, "error_code": 1},
        ).limit(limit)
    )
    requeued = []
    failed = []
    now = datetime.utcnow()
    for d in docs:
        jid = d.get("job_id")
        if not jid:
            continue
        countdown = 0
        next_retry_at = d.get("next_retry_at")
        if next_retry_at:
            try:
                # Mongo may return tz-aware; normalize to naive UTC
                if getattr(next_retry_at, "tzinfo", None) is not None:
                    next_retry_at = next_retry_at.replace(tzinfo=None)
                countdown = max(0, int((next_retry_at - now).total_seconds()))
            except Exception:
                countdown = 0
        # If SMTP timeout, switching provider often helps; mirror dlq_processor behavior.
        switch = (d.get("error_code") == "SMTP_TIMEOUT")
        try:
            requeued.append(_requeue_job_from_dlq(jid, countdown=countdown, switch_provider=switch))
        except Exception as e:
            failed.append({"job_id": jid, "error": str(e)})
    return {"message": "DLQ reprocess scheduled", "attempted": len(docs), "scheduled": len(requeued), "failures": failed}


# ── PROVIDERS MANAGEMENT ENDPOINTS ────────────────────────────────────────────

@app.get("/providers")
def get_providers(channel: Optional[str] = None):
    """List all providers. Optionally filter by channel."""
    query = {}
    if channel:
        query["channel"] = channel
    docs = list(providers_collection.find(query, {"_id": 0}).sort([("channel", 1), ("priority", 1)]))
    return {"count": len(docs), "providers": docs}


@app.post("/providers")
def upsert_provider(provider: dict):
    """
    Add or update a provider entry.
    Required fields: provider_id, provider_name, channel, priority, is_active
    Example body:
      {
        "provider_id": "sendgrid", "provider_name": "SendGrid",
        "channel": "email", "priority": 1, "is_active": true,
        "max_retries": 3, "timeout_ms": 5000
      }
    """
    provider_id = provider.get("provider_id")
    if not provider_id:
        raise HTTPException(status_code=400, detail="provider_id is required")
    provider_name = provider.get("provider_name")
    if provider_name not in _SUPPORTED_PROVIDER_NAMES:
        supported = ", ".join(sorted(_SUPPORTED_PROVIDER_NAMES))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported provider_name '{provider_name}'. Supported providers: {supported}",
        )
    providers_collection.update_one(
        {"provider_id": provider_id},
        {"$set": {**provider, "updated_at": datetime.utcnow() + timedelta(hours=5, minutes=30)}},
        upsert=True,
    )
    return {"message": f"Provider '{provider_id}' saved.", "provider_id": provider_id}


@app.get("/unsubscribe")
def unsubscribe(user_id: str, event_type: str = None):
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    if event_type:
        result = preferences_collection.update_one(
            {
                "user_id": user_id,
                "event_channel_preferences.event_type": event_type
            },
            {
                "$pull": {
                    "event_channel_preferences.$.channels": "email"
                },
                "$set": {"updated_at": now_ist}
            }
        )

        if result.matched_count == 0:
            preferences_collection.update_one(
                {"user_id": user_id},
                {
                    "$push": {
                        "event_channel_preferences": {
                            "event_type": event_type,
                            "channels": []
                        }
                    },
                    "$set": {"updated_at": now_ist}
                },
                upsert=True
            )

    else:
        preferences_collection.update_one(
            {"user_id": user_id},
            {"$set": {"channels.email": False, "updated_at": now_ist}},
            upsert=True
        )

    return HTMLResponse("<h2>Unsubscribed successfully</h2>")


# ── SENDGRID WEBHOOK ──────────────────────────────────────────────────────────
#
# SendGrid sends a POST to this endpoint for every email event.
# Events we care about:
#   delivered  → update delivered_at in delivery_logs
#   open       → update read_at in delivery_logs
#   bounce     → mark delivery_log status as FAILED
#   spamreport → mark delivery_log status as SPAM

def verify_sendgrid_signature(payload: bytes, signature: str, timestamp: str) -> bool:
    """
    Verifies the SendGrid webhook signature to ensure the request is genuine.
    SendGrid signs using HMAC-SHA256 over (timestamp + payload).
    """
    webhook_key = os.getenv("SENDGRID_WEBHOOK_KEY")
    if not webhook_key:
        return True  # Skip in local dev

    signed_payload = timestamp.encode() + payload
    mac = hmac.new(webhook_key.encode(), signed_payload, hashlib.sha256)
    expected = mac.hexdigest()
    return hmac.compare_digest(expected, signature)


def _message_id_candidates(raw_id) -> list[str]:
    candidates: list[str] = []
    if raw_id in (None, ""):
        return candidates

    text = str(raw_id).strip()
    if not text:
        return candidates

    variants = [
        text,
        text.split(".")[0],
        text.strip("<>"),
    ]
    if "." in text:
        variants.append(text.strip("<>").split(".")[0])

    seen = set()
    for value in variants:
        value = str(value).strip()
        if value and value not in seen:
            seen.add(value)
            candidates.append(value)
    return candidates


def _recipient_value_candidates(raw_value) -> list[str]:
    candidates: list[str] = []
    if raw_value in (None, ""):
        return candidates

    text = str(raw_value).strip()
    if not text:
        return candidates

    normalized = [text, text.lower()]
    seen = set()
    for value in normalized:
        if value and value not in seen:
            seen.add(value)
            candidates.append(value)
    return candidates


def _event_nested_dicts(event: dict) -> list[dict]:
    nested = [event]
    for key in ("custom_args", "unique_args", "data", "message", "payload", "status", "value"):
        value = event.get(key)
        if isinstance(value, dict):
            nested.append(value)
    return nested


def _event_pick(event: dict, *keys: str):
    for candidate in _event_nested_dicts(event):
        for key in keys:
            value = candidate.get(key)
            if value not in (None, ""):
                return value
    return None


def _find_delivery_log(
    channel: Optional[str],
    provider_message_id=None,
    job_id=None,
    provider_sid=None,
    provider_hash=None,
    recipient_user_id=None,
    recipient_address=None,
) -> Optional[dict]:
    base_query = {}
    if channel:
        # Be tolerant of historical data where channel was stored as "EMAIL"/"WHATSAPP".
        base_query["channel"] = {"$regex": f"^{re.escape(str(channel))}$", "$options": "i"}

    id_filters = []
    msg_candidates = _message_id_candidates(provider_message_id)
    if msg_candidates:
        id_filters.append({"provider_message_id": {"$in": msg_candidates}})

    sid_candidates = _message_id_candidates(provider_sid)
    if sid_candidates:
        id_filters.append({"provider_message_sid": {"$in": sid_candidates}})

    hash_candidates = _message_id_candidates(provider_hash)
    if hash_candidates:
        id_filters.append({"provider_message_hash": {"$in": hash_candidates}})

    if id_filters:
        query = dict(base_query)
        query["$or"] = id_filters
        doc = delivery_logs_collection.find_one(query, {"_id": 0}, sort=[("created_at", -1)])
        if doc:
            return doc

    if not job_id:
        return None

    fallback_query = dict(base_query)
    fallback_query["job_id"] = str(job_id)

    recipient_user_candidates = _recipient_value_candidates(recipient_user_id)
    if recipient_user_candidates:
        fallback_query["recipient_user_id"] = {"$in": recipient_user_candidates}

    recipient_address_candidates = _recipient_value_candidates(recipient_address)
    if recipient_address_candidates:
        fallback_query["recipient_address"] = {"$in": recipient_address_candidates}

    docs = list(
        delivery_logs_collection
        .find(fallback_query, {"_id": 0})
        .sort("created_at", -1)
        .limit(2)
    )
    if len(docs) == 1:
        return docs[0]
    if len(docs) > 1 and (recipient_user_candidates or recipient_address_candidates):
        return docs[0]
    return None


def _compute_latency_ms(log: Optional[dict], event_time: datetime) -> Optional[int]:
    if not log:
        return None

    sent_at = log.get("sent_at")
    if not sent_at:
        return None

    try:
        if getattr(sent_at, "tzinfo", None) is not None:
            sent_at = sent_at.replace(tzinfo=None)
        delta = event_time - sent_at
        return max(0, int(delta.total_seconds() * 1000))
    except Exception:
        return None


@app.post("/webhooks/sendgrid")
async def sendgrid_webhook(request: Request):
    """
    Receives SendGrid event webhook POSTs.
    Each request body is a JSON array of event objects.

    Each event has at minimum:
      - event:         "delivered" | "open" | "bounce" | "spamreport" | ...
      - sg_message_id: SendGrid message ID (matches provider_message_id in delivery_logs)
      - timestamp:     Unix epoch seconds
    """
    signature = request.headers.get("X-Twilio-Email-Event-Webhook-Signature", "")
    timestamp = request.headers.get("X-Twilio-Email-Event-Webhook-Timestamp", "")
    raw_body  = await request.body()

    if signature and not verify_sendgrid_signature(raw_body, signature, timestamp):
        raise HTTPException(status_code=403, detail="Invalid webhook signature")

    try:
        events = json.loads(raw_body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    processed = 0
    skipped   = 0

    for event in events:
        sg_event        = event.get("event", "")
        raw_msg_id      = event.get("sg_message_id", "")
        provider_msg_id = _event_pick(event, "sg_message_id", "message_id", "messageId") or raw_msg_id or None
        event_job_id    = _event_pick(event, "job_id", "_job_id")
        event_user_id   = _event_pick(event, "recipient_user_id", "_recipient_user_id", "user_id")
        event_address   = _event_pick(event, "recipient_address", "_recipient_address", "email")
        event_ts        = event.get("timestamp")
        now_ist         = datetime.utcnow() + timedelta(hours=5, minutes=30)

        event_time = (
            datetime.utcfromtimestamp(event_ts) + timedelta(hours=5, minutes=30)
            if event_ts else now_ist
        )

        log = _find_delivery_log(
            "email",
            provider_message_id=provider_msg_id,
            job_id=event_job_id,
            recipient_user_id=event_user_id,
            recipient_address=event_address,
        )
        if not log:
            skipped += 1
            continue

        update_filter = {"log_id": log["log_id"]}
        sync_fields = {}
        provider_candidates = _message_id_candidates(provider_msg_id)
        if provider_candidates and log.get("provider_message_id") not in provider_candidates:
            sync_fields["provider_message_id"] = provider_candidates[0]

        if sg_event == "delivered":
            latency_ms = _compute_latency_ms(log, event_time)

            delivery_logs_collection.update_one(
                update_filter,
                {"$set": {
                    "status":       "DELIVERED",
                    "delivered_at": event_time,
                    "latency_ms":   latency_ms,
                    **sync_fields,
                }},
            )

            notification_jobs_collection.update_one(
                {"job_id": log["job_id"]},
                {"$set": {"status": "DELIVERED", "updated_at": now_ist}},
            )
            processed += 1

        elif sg_event == "open":
            update_doc = {
                "status": "READ",
                "read_at": event_time,
                **sync_fields,
            }
            if not log.get("delivered_at"):
                update_doc["delivered_at"] = event_time
                update_doc["latency_ms"] = _compute_latency_ms(log, event_time)
            delivery_logs_collection.update_one(
                update_filter,
                {"$set": update_doc},
            )
            notification_jobs_collection.update_one(
                {"job_id": log["job_id"]},
                {"$set": {"status": "READ", "updated_at": now_ist}},
            )
            processed += 1

        elif sg_event == "bounce":
            bounce_reason = event.get("reason", "Bounced")
            delivery_logs_collection.update_one(
                update_filter,
                {"$set": {
                    "status": "FAILED",
                    "error":  {"message": f"Bounce: {bounce_reason}"},
                    **sync_fields,
                }},
            )
            notification_jobs_collection.update_one(
                {"job_id": log["job_id"]},
                {"$set": {"status": "FAILED", "updated_at": now_ist}},
            )
            processed += 1

        elif sg_event == "spamreport":
            delivery_logs_collection.update_one(
                update_filter,
                {"$set": {"status": "SPAM", **sync_fields}},
            )
            notification_jobs_collection.update_one(
                {"job_id": log["job_id"]},
                {"$set": {"status": "SPAM", "updated_at": now_ist}},
            )
            processed += 1

        else:
            skipped += 1

    print(f"[WEBHOOK] SendGrid: {len(events)} event(s) — {processed} processed, {skipped} skipped")

    return {"received": len(events), "processed": processed, "skipped": skipped}


# ── ULTRAMSG (WHATSAPP) WEBHOOK ───────────────────────────────────────────────
#
# UltraMsg can POST delivery/read events. Payload shapes vary; this endpoint
# is intentionally tolerant and attempts to extract:
#   - message id (provider_message_id)
#   - status/event (delivered/read/failed/sent)
#   - timestamp
#
# Optional verification:
#   - set ULTRAMSG_WEBHOOK_TOKEN in .env and send header X-UltraMsg-Token
#

def _verify_ultramsg_webhook(request: Request) -> bool:
    expected = os.getenv("ULTRAMSG_WEBHOOK_TOKEN")
    if not expected:
        return True  # local dev
    got = request.headers.get("X-UltraMsg-Token", "")
    return hmac.compare_digest(got, expected)


def _extract_ultramsg_events(payload) -> list[dict]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        # Some providers wrap events
        for key in ("events", "data", "messages", "statuses"):
            v = payload.get(key)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]
        return [payload]
    return []


def _ultramsg_nested_candidates(ev: dict) -> list[dict]:
    candidates: list[dict] = [ev]
    for key in ("data", "message", "payload", "status", "value"):
        nested = ev.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
    return candidates


def _ultramsg_pick(ev: dict, *keys: str):
    for candidate in _ultramsg_nested_candidates(ev):
        for key in keys:
            value = candidate.get(key)
            if value not in (None, ""):
                return value
    return None


def _ultramsg_canonical_status(ev: dict) -> Optional[str]:
    raw_status = _ultramsg_pick(ev, "status", "event_type", "event", "type", "state", "messageStatus", "message_status")
    if raw_status is not None:
        status = str(raw_status).strip().lower()
        if status in {"delivered", "deliver", "delivery"}:
            return "DELIVERED"
        if status in {"read", "seen", "opened", "open", "viewed"}:
            return "READ"
        if status in {"failed", "error", "undelivered"}:
            return "FAILED"
        if status in {"sent", "queued", "accepted"}:
            return "SENT"
        # UltraMsg webhook frequently uses event_type=message_ack with ack as a string.
        if status in {"message_ack", "ack"}:
            ack_val = _ultramsg_pick(ev, "ack")
            if ack_val is not None:
                ack_s = str(ack_val).strip().lower()
                if ack_s in {"read", "seen"}:
                    return "READ"
                if ack_s in {"device", "delivered"}:
                    return "DELIVERED"
                if ack_s in {"server", "sent", "queued"}:
                    return "SENT"

    ack = _ultramsg_pick(ev, "ack")
    if ack is not None:
        try:
            ack_num = int(str(ack).strip())
        except Exception:
            ack_num = None
        if ack_num is not None:
            if ack_num == 1:
                return "SENT"
            if ack_num == 2:
                return "DELIVERED"
            if ack_num >= 3:
                return "READ"

    return None


@app.post("/webhooks/ultramsg")
async def ultramsg_webhook(request: Request):
    if not _verify_ultramsg_webhook(request):
        raise HTTPException(status_code=403, detail="Invalid webhook token")

    raw_body = await request.body()
    # Store raw inbound webhook for debugging payload shape
    try:
        ultramsg_inbound_collection.insert_one({
            "inbound_id": f"um_{uuid.uuid4().hex[:12]}",
            "headers": dict(request.headers),
            "raw_body": raw_body.decode("utf-8", errors="replace")[:20000],
            "received_at": datetime.utcnow(),
        })
    except Exception:
        pass

    try:
        payload = json.loads(raw_body.decode("utf-8", errors="replace"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    events = _extract_ultramsg_events(payload)
    processed = 0
    skipped = 0

    for ev in events:
        # Try common id fields
        provider_msg_id = (
            _ultramsg_pick(
                ev,
                "id",
                "message_id",
                "msgid",
                "messageId",
                "messageID",
                "wamid",
                "sid",
            )
        )
        provider_sid = _ultramsg_pick(ev, "sid")
        provider_hash = _ultramsg_pick(ev, "hash")
        event_job_id = _ultramsg_pick(ev, "job_id", "_job_id")
        event_user_id = _ultramsg_pick(ev, "recipient_user_id", "_recipient_user_id", "user_id")
        event_address = _ultramsg_pick(ev, "to", "chatId", "recipient_address", "_recipient_address", "wa_number", "phone")
        ts = _ultramsg_pick(ev, "timestamp", "time", "ts", "created_at", "date")
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        event_time = now_ist
        if ts is not None:
            try:
                # If numeric epoch seconds
                if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.isdigit()):
                    event_time = datetime.utcfromtimestamp(int(ts)) + timedelta(hours=5, minutes=30)
            except Exception:
                event_time = now_ist

        # Map provider status to our canonical status
        canonical = _ultramsg_canonical_status(ev)
        if not canonical:
            skipped += 1
            continue

        log = _find_delivery_log(
            "whatsapp",
            provider_message_id=provider_msg_id,
            job_id=event_job_id,
            provider_sid=provider_sid,
            provider_hash=provider_hash,
            recipient_user_id=event_user_id,
            recipient_address=event_address,
        )
        if not log:
            skipped += 1
            continue

        update = {"status": canonical}
        provider_candidates = _message_id_candidates(provider_msg_id)
        if provider_candidates and log.get("provider_message_id") not in provider_candidates:
            update["provider_message_id"] = provider_candidates[0]
        sid_candidates = _message_id_candidates(provider_sid)
        if sid_candidates and log.get("provider_message_sid") not in sid_candidates:
            update["provider_message_sid"] = sid_candidates[0]
        hash_candidates = _message_id_candidates(provider_hash)
        if hash_candidates and log.get("provider_message_hash") not in hash_candidates:
            update["provider_message_hash"] = hash_candidates[0]
        if canonical == "DELIVERED":
            update["delivered_at"] = event_time
            update["latency_ms"] = _compute_latency_ms(log, event_time)
        elif canonical == "READ":
            update["read_at"] = event_time
            if not log.get("delivered_at"):
                update["delivered_at"] = event_time
                update["latency_ms"] = _compute_latency_ms(log, event_time)
        elif canonical == "FAILED":
            update["error"] = {
                "message": _ultramsg_pick(ev, "reason", "error", "description") or "WhatsApp delivery failed"
            }

        delivery_logs_collection.update_one(
            {"log_id": log["log_id"]},
            {"$set": update},
        )

        # Update job status for tracking/monitoring
        notification_jobs_collection.update_one(
            {"job_id": log.get("job_id")},
            {"$set": {"status": canonical, "updated_at": now_ist}},
        )

        # Client webhook fan-out (best-effort)
        _emit_client_webhook(
            log.get("client_id"),
            {
                "type": "NOTIFICATION_STATUS",
                "job_id": log.get("job_id"),
                "client_id": log.get("client_id"),
                "event_type": log.get("event_type"),
                "channel": "whatsapp",
                "status": canonical,
                "provider": log.get("provider"),
                "provider_message_id": provider_msg_id,
                "timestamp": event_time.isoformat() if hasattr(event_time, "isoformat") else str(event_time),
            },
        )

        processed += 1

    print(f"[WEBHOOK] UltraMsg: {len(events)} event(s) — {processed} processed, {skipped} skipped")
    return {"received": len(events), "processed": processed, "skipped": skipped}


@app.get("/webhooks/ultramsg/latest")
def ultramsg_latest(_: str = Depends(_require_bearer_token)):
    """Debug endpoint: fetch the most recent inbound UltraMsg webhook payload."""
    doc = ultramsg_inbound_collection.find_one({}, {"_id": 0}, sort=[("received_at", -1)])
    if not doc:
        return {"found": False}
    return {"found": True, "latest": doc}

# ── FAILURE ANALYTICS ROUTE ───────────────────────────────────────────────────

@app.get("/failure-analytics")
def get_failure_analytics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    Returns failure analytics for the dashboard:
      - Total notifications dispatched in the date range
      - Failure rate (%) and delivery rate (%)
      - Per-event-type failure counts (for the table)

    Query params:
      start_date  ISO date string, e.g. 2026-04-25  (inclusive)
      end_date    ISO date string, e.g. 2026-04-26  (inclusive, end of day)
    """

    # ── Build date filter ────────────────────────────────────────────────────
    date_filter: dict = {}
    try:
        if start_date:
            dt_start = datetime.strptime(start_date, "%Y-%m-%d")
            date_filter["$gte"] = dt_start
        if end_date:
            dt_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
            date_filter["$lte"] = dt_end
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {exc}")

    match_clause: dict = {}
    if date_filter:
        match_clause["created_at"] = date_filter

    # ── Aggregate delivery_logs ──────────────────────────────────────────────
    pipeline = [
        {"$match": match_clause},
        {
            "$group": {
                "_id": {
                    "event_type": "$event_type",
                    "status":     "$status",
                },
                "count": {"$sum": 1},
            }
        },
        {
            "$group": {
                "_id":    "$_id.event_type",
                "totals": {
                    "$push": {
                        "status": "$_id.status",
                        "count":  "$count",
                    }
                },
            }
        },
        {
            "$project": {
                "_id":        0,
                "event_type": "$_id",
                "totals":     1,
            }
        },
        {"$sort": {"event_type": 1}},
    ]

    results = list(delivery_logs_collection.aggregate(pipeline))

    # ── Reshape into per-event-type summary ──────────────────────────────────
    rows = []
    grand_total    = 0
    grand_failures = 0

    for doc in results:
        event_type = doc.get("event_type") or "UNKNOWN"
        sent   = 0
        failed = 0
        for t in doc.get("totals", []):
            if t["status"] == "SENT":
                sent += t["count"]
            elif t["status"] == "FAILED":
                failed += t["count"]

        total = sent + failed
        grand_total    += total
        grand_failures += failed

        if total > 0:
            rows.append({
                "event_type":    event_type,
                "total":         total,
                "sent":          sent,
                "failures":      failed,
                "failure_rate":  round(failed / total * 100, 2),
                "delivery_rate": round(sent   / total * 100, 2),
            })

    # ── Grand totals ─────────────────────────────────────────────────────────
    grand_sent          = grand_total - grand_failures
    grand_failure_rate  = round(grand_failures / grand_total * 100, 2) if grand_total else 0.0
    grand_delivery_rate = round(grand_sent     / grand_total * 100, 2) if grand_total else 0.0

    return {
        "total":         grand_total,
        "sent":          grand_sent,
        "failures":      grand_failures,
        "failure_rate":  grand_failure_rate,
        "delivery_rate": grand_delivery_rate,
        "by_event_type": rows,
    }
