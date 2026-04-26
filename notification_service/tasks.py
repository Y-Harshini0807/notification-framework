"""Celery worker — consumes notification jobs from RabbitMQ queues,
calls the real provider for each channel, writes delivery_logs to MongoDB.

Start workers (from project root, inside venv):
  celery -A tasks worker --loglevel=info --concurrency=4 -Q email-notify-q,sms-notify-q,whatsapp-notify-q,push-notify-q

Provider credentials go in .env — see .env.example for all keys.
To test only ONE channel, fill in just that channel's keys and leave the rest blank.
"""

from celery_config import celery_app
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Optional
import requests as http_requests
import uuid
import os
import smtplib
import html
import time
import re
import mimetypes
from email.mime.text import MIMEText
from urllib.parse import urlparse, parse_qs

load_dotenv()

# ── DB connection inside worker process ──────────────────────────────────────
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
_mongo_client = MongoClient(MONGO_URL)
_db = _mongo_client["notification_db"]
notification_jobs_collection = _db["notification_jobs"]
delivery_logs_collection     = _db["delivery_logs"]
queue_controls_collection    = _db["queue_controls"]
queue_rate_counters_collection = _db["queue_rate_counters"]
clients_collection           = _db["clients"]
webhook_calls_collection     = _db["webhook_calls"]


def _emit_client_webhook(event: dict) -> None:
    """
    Best-effort webhook dispatch to client-configured webhook_url.
    Never raises.
    """
    try:
        client_id = (event.get("client_id") or "").strip()
        if not client_id:
            return
        client = clients_collection.find_one({"client_id": client_id}, {"_id": 0, "webhook_url": 1})
        url = (client or {}).get("webhook_url")
        if not url:
            return

        started_at = datetime.utcnow()
        try:
            resp = http_requests.post(url, json=event, timeout=5)
            ok = 200 <= resp.status_code < 300
            webhook_calls_collection.insert_one({
                "call_id":    f"wh_{uuid.uuid4().hex[:12]}",
                "client_id":  client_id,
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
                "client_id":  client_id,
                "url":        url,
                "status":     "ERROR",
                "error":      str(e),
                "event":      event,
                "created_at": started_at,
            })
    except Exception:
        return


def _get_channel_control(channel: str) -> dict:
    """Fetch (or initialize) queue control state for a channel."""
    channel = (channel or "").strip().lower()
    if not channel:
        channel = "email"
    doc = queue_controls_collection.find_one({"channel": channel})
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

        merged = {
            "channel": channel,
            "paused": False,
            # rate_limit is stored as jobs/min (0 means "unset"/no admin throttling)
            "rate_limit": 0,
            "active_queue": f"{channel}-notify-q",
            "backup_queue": None,
            "backup_enabled": True,
            **doc,
        }
        missing = {k: v for k, v in merged.items() if k not in doc}
        if missing:
            queue_controls_collection.update_one(
                {"channel": channel},
                {"$set": {**missing, "updated_at": datetime.utcnow()}},
            )
        return merged
    # Default: unpaused, rate_limit unset (0 jobs/min)
    doc = {
        "channel": channel,
        "paused": False,
        "rate_limit": 0,
        "active_queue": f"{channel}-notify-q",
        "backup_queue": None,
        "backup_enabled": True,
        "updated_at": datetime.utcnow(),
    }
    queue_controls_collection.update_one({"channel": channel}, {"$setOnInsert": doc}, upsert=True)
    return doc


def _enforce_channel_rate_limit(channel: str, jobs_per_min: int) -> int:
    """
    Global rate limiter across ALL workers for a channel.
    Uses MongoDB atomic counter per-minute window.

    Returns countdown seconds to wait (0 means allowed).
    """
    try:
        per_min = int(jobs_per_min)
    except Exception:
        per_min = 0

    if per_min <= 0:
        return 0  # treated as "no throttling"

    now = time.time()
    # Fixed 60s window: [t, t+60)
    window_start = int(now // 60) * 60
    key = {"channel": channel, "window_start": window_start}
    update = {"$inc": {"count": 1}, "$setOnInsert": {"created_at": datetime.utcnow()}}
    doc = queue_rate_counters_collection.find_one_and_update(
        key,
        update,
        upsert=True,
        return_document=True,
    )
    # PyMongo may return None for some server configs; fall back to allow.
    count = int((doc or {}).get("count", 1))
    if count <= per_min:
        return 0
    # Wait until next minute window
    return max(1, (window_start + 60) - int(now))

# ════════════════════════════════════════════════════════════════════════════
#  PROVIDER FUNCTIONS
#  Each function receives (recipient_address, content) and returns a dict:
#    { provider, provider_message_id, status, error }
#  Raise an exception on failure — the task will retry automatically.
# ════════════════════════════════════════════════════════════════════════════

def build_email_footer(user_id: str, event_type: Optional[str]) -> str:
    """Small HTML block appended to SendGrid messages (ref + safe text)."""
    uid = html.escape(str(user_id or "unknown"))
    evt = html.escape(str(event_type or "notification"))
    return (
        '<hr style="border:none;border-top:1px solid #eee;margin:1.5em 0;" />'
        f'<p style="font-size:11px;color:#888;">Ref: user {uid} · {evt}</p>'
    )


def _extract_google_drive_target(url: str) -> tuple[Optional[str], Optional[str]]:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    if "drive.google.com" not in host and "docs.google.com" not in host:
        return None, None

    qs_file_id = parse_qs(parsed.query).get("id")
    if qs_file_id and qs_file_id[0]:
        return qs_file_id[0], "file"

    match = re.search(r"/file/d/([^/]+)", parsed.path or "")
    if match:
        return match.group(1), "file"

    match = re.search(r"/document/d/([^/]+)", parsed.path or "")
    if match:
        return match.group(1), "document"

    match = re.search(r"/spreadsheets/d/([^/]+)", parsed.path or "")
    if match:
        return match.group(1), "spreadsheet"

    match = re.search(r"/presentation/d/([^/]+)", parsed.path or "")
    if match:
        return match.group(1), "presentation"

    return None, None


def _is_google_workspace_link(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    return "drive.google.com" in host or "docs.google.com" in host


def _normalize_provider_media_url(url: str) -> tuple[str, Optional[str]]:
    file_id, kind = _extract_google_drive_target(url)
    if not file_id:
        return url, None
    if kind == "file":
        # Providers need a direct fetchable file URL, not the Drive preview HTML page.
        return f"https://drive.google.com/uc?export=download&id={file_id}", None
    if kind == "document":
        return f"https://docs.google.com/document/d/{file_id}/export?format=docx", ".docx"
    if kind == "spreadsheet":
        return f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", ".xlsx"
    if kind == "presentation":
        return f"https://docs.google.com/presentation/d/{file_id}/export/pptx", ".pptx"
    return url, None


def _guess_extension_from_url(url: str) -> str:
    path = urlparse(url or "").path or ""
    ext = os.path.splitext(path)[1]
    return ext.lower()


def _normalized_attachment_filename(
    name: Optional[str],
    mime_type: Optional[str],
    url: str,
    forced_ext: Optional[str] = None,
) -> str:
    candidate = (name or "").strip()
    ext = os.path.splitext(candidate)[1].lower() if candidate else ""
    if forced_ext:
        base = os.path.splitext(candidate)[0] if candidate else "attachment"
        return f"{base}{forced_ext}"
    if ext:
        return candidate

    guessed_ext = ""
    if mime_type:
        guessed_ext = mimetypes.guess_extension(mime_type.split(";")[0].strip().lower()) or ""
    if not guessed_ext:
        guessed_ext = _guess_extension_from_url(url)
    if not guessed_ext:
        # UltraMsg requires a filename; prefer a binary-safe default over a bare name.
        guessed_ext = ".bin"

    base = candidate or "attachment"
    return f"{base}{guessed_ext}"


def _looks_like_html_response(response) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" in content_type:
        return True
    if "application/xhtml+xml" in content_type:
        return True
    return False


def send_email(recipient_address: str, content: dict) -> dict:
    """
    SendGrid REST API.
    .env keys needed:
      SENDGRID_API_KEY   — starts with SG.
      SENDGRID_FROM      — verified sender address e.g. noreply@yourdomain.com
    """
    api_key    = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("SENDGRID_FROM", "noreply@example.com")
    if not api_key:
        raise ValueError("SENDGRID_API_KEY not set in .env")

    subject   = content.get("subject", "Notification")
    body_text = content.get("body")  or content.get("message")
    # Try known keys first
    body_text = content.get("body") or content.get("message")

    # Fallback: take ANY string value from content dict
    if not body_text:
        for v in content.values():
            if isinstance(v, str) and v.strip():
                body_text = v
                break
    # Final safety
    if not body_text:
        raise ValueError(f"Invalid email content: {content}")
    
    user_id    = content.get("user_id") or "unknown"
    event_type = content.get("event_type")
    base_html  = content.get("html_body") or f"<p>{body_text}</p>"
    footer     = build_email_footer(user_id, event_type)
    body_html  = base_html + footer
    custom_args = {}
    for key in ("_job_id", "_request_id", "_client_id", "_channel", "_recipient_user_id", "_recipient_address"):
        value = content.get(key)
        if value:
            custom_args[key.lstrip("_")] = str(value)

    # Optional attachments:
    # - If attachments are provided as URLs, we attach small files and link large ones.
    # - SendGrid attachment limit is ~30MB; we default to a safer 20MB per file.
    attachments = []
    attachment_links = []
    max_attach_mb = int(os.getenv("EMAIL_MAX_ATTACHMENT_MB", "20"))
    max_attach_bytes = max_attach_mb * 1024 * 1024
    for a in (content.get("attachments") or []):
        if not isinstance(a, dict):
            continue
        url = a.get("url")
        if not url:
            continue
        original_url = str(url)
        if _is_google_workspace_link(original_url):
            attachment_links.append({"name": a.get("name") or "attachment", "url": original_url})
            continue
        normalized_url, forced_ext = _normalize_provider_media_url(original_url)
        name = _normalized_attachment_filename(a.get("name"), a.get("mime_type"), normalized_url, forced_ext)
        delivery_mode = str(a.get("delivery_mode") or "").strip().lower()
        if delivery_mode == "link_only":
            attachment_links.append({"name": name, "url": original_url})
            continue
        size_bytes = a.get("size_bytes")
        if isinstance(size_bytes, (int, float)) and int(size_bytes) > max_attach_bytes:
            attachment_links.append({"name": name, "url": original_url})
            continue
        try:
            r = http_requests.get(normalized_url, timeout=20, allow_redirects=True)
            if r.status_code >= 400:
                attachment_links.append({"name": name, "url": original_url})
                continue
            if _looks_like_html_response(r):
                attachment_links.append({"name": name, "url": original_url})
                continue
            data = r.content
            if len(data) > max_attach_bytes:
                attachment_links.append({"name": name, "url": original_url})
                continue
            import base64
            attachments.append({
                "content": base64.b64encode(data).decode("utf-8"),
                "type": a.get("mime_type") or "application/octet-stream",
                "filename": name,
                "disposition": "attachment",
            })
        except Exception:
            attachment_links.append({"name": name, "url": str(url)})

    if attachment_links:
        extra = "\n\nAttachments:\n" + "\n".join([f"- {x['name']}: {x['url']}" for x in attachment_links])
        body_text = (body_text or "").rstrip() + extra
        body_html = (body_html or "").rstrip() + "<br/><br/><strong>Attachments:</strong><br/>" + "<br/>".join(
            [f"- {html.escape(x['name'])}: <a href=\"{html.escape(x['url'])}\">{html.escape(x['url'])}</a>" for x in attachment_links]
        )

    payload = {
        "personalizations": [{
            "to": [{"email": recipient_address}],
            "subject": subject,
            "custom_args": custom_args,
        }],
        "from":    {"email": from_email},
        "content": [
            {"type": "text/plain", "value": body_text},
            {"type": "text/html",  "value": body_html},
        ],
        "tracking_settings": {
            "open_tracking": {"enable": True}
        },
    }
    if attachments:
        payload["attachments"] = attachments

    response = http_requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=10,
    )

    if response.status_code == 202:
        msg_id = response.headers.get("X-Message-Id", f"sg_{uuid.uuid4().hex[:10]}")
        return {"provider": "sendgrid", "provider_message_id": msg_id, "status": "SENT", "error": None}

    raise RuntimeError(f"SendGrid {response.status_code}: {response.text}")

def send_email_smtp(recipient_address: str, content: dict) -> dict:
    sender = os.getenv("SMTP_EMAIL")
    password = os.getenv("SMTP_PASSWORD")

    if not sender or not password:
        raise ValueError("SMTP credentials missing")

    subject = content.get("subject", "Notification")
    body    = content.get("body", "Hello")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = recipient_address

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)

    return {
        "provider": "SMTP",
        "provider_message_id": f"smtp_{uuid.uuid4().hex[:10]}",
        "status": "SENT",
        "error": None
    }
    
def send_sms(recipient_address: str, content: dict) -> dict:
    """
    SendFire SMS API.
    .env keys needed:
      SENDFIRE_API_KEY    — your SendFire API key
      SENDFIRE_SENDER_ID  — alphanumeric sender ID e.g. NOTIFY

    Character limit handling:
      - main.py enforce_channel_limits() splits long bodies into sms_segments list.
      - If sms_segments is present, each segment is sent as a separate SMS.
      - Falls back to body field for single-segment messages.
      - Body > 480 chars is rejected at API layer before reaching here.
    """
    api_key   = os.getenv("SENDFIRE_API_KEY")
    sender_id = os.getenv("SENDFIRE_SENDER_ID", "NOTIFY")
    if not api_key:
        raise ValueError("SENDFIRE_API_KEY not set in .env")

    # Use pre-split segments if available (set by enforce_channel_limits in main.py)
    segments = content.get("sms_segments")
    if not segments:
        body = content.get("body", content.get("subject", "Notification"))
        segments = [body[:160]]   # safety fallback — should not be needed

    message_ids = []
    for i, segment in enumerate(segments):
        response = http_requests.post(
            "https://api.sendfire.co/sms/send",
            json={"api_key": api_key, "sender_id": sender_id, "to": recipient_address, "message": segment},
            timeout=10,
        )
        data = response.json()
        if response.status_code == 200 and data.get("status") == "success":
            message_ids.append(str(data.get("message_id", uuid.uuid4().hex[:10])))
            print(f"[SMS] Sent segment {i+1}/{len(segments)} to {recipient_address}")
        else:
            raise RuntimeError(f"SendFire segment {i+1} failed {response.status_code}: {data}")

    # Return combined message_id (all segment IDs joined)
    return {
        "provider":            "sendfire",
        "provider_message_id": ",".join(message_ids),
        "status":              "SENT",
        "error":               None,
    }

def send_whatsapp(recipient_address: str, content: dict) -> dict:
    """
    UltraMsg WhatsApp API.
    .env keys needed:
      ULTRAMSG_INSTANCE_ID  — e.g. instance12345
      ULTRAMSG_TOKEN        — your UltraMsg token
    """
    instance_id = os.getenv("ULTRAMSG_INSTANCE_ID")
    token       = os.getenv("ULTRAMSG_TOKEN")
    if not instance_id or not token:
        raise ValueError("ULTRAMSG_INSTANCE_ID or ULTRAMSG_TOKEN not set in .env")

    body = content.get("body") or ""
    # UltraMsg expects international format with '+' (or chatId like 1415...@c.us).
    # The instance UI examples use "+<countrycode><number>".
    phone_raw = (recipient_address or "").strip()
    if not phone_raw:
        raise ValueError("Missing WhatsApp recipient address")
    phone = phone_raw
    if phone.isdigit():
        phone = f"+{phone}"

    # If attachments are present, attempt to send as UltraMsg media (document/video),
    # and always send a text message containing links as a fallback.
    send_media = str(os.getenv("ULTRAMSG_SEND_MEDIA", "1")).lower() in {"1", "true", "yes"}
    max_media_mb = int(os.getenv("WHATSAPP_MAX_MEDIA_MB", "64"))
    max_media_bytes = max_media_mb * 1024 * 1024
    attachment_urls = []
    sent_media_ids = []
    if send_media:
        for a in (content.get("attachments") or []):
            if not isinstance(a, dict):
                continue
            url = a.get("url")
            if not url:
                continue
            original_url = str(url)
            if _is_google_workspace_link(original_url):
                attachment_urls.append((a.get("name") or "file", original_url))
                continue
            normalized_url, forced_ext = _normalize_provider_media_url(original_url)
            name = _normalized_attachment_filename(a.get("name"), a.get("mime_type"), normalized_url, forced_ext)
            mime_type = (a.get("mime_type") or "").lower()
            delivery_mode = str(a.get("delivery_mode") or "").strip().lower()
            if delivery_mode == "link_only":
                attachment_urls.append((name, original_url))
                continue
            size_bytes = a.get("size_bytes")
            if isinstance(size_bytes, (int, float)) and int(size_bytes) > max_media_bytes:
                attachment_urls.append((name, original_url))
                continue
            endpoint = "document"
            if mime_type.startswith("video/") or name.lower().endswith((".mp4", ".mov", ".mkv", ".webm")):
                endpoint = "video"
            try:
                resp = http_requests.post(
                    f"https://api.ultramsg.com/{instance_id}/messages/{endpoint}",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data={
                        "token": token,
                        "to": phone,
                        # UltraMsg commonly accepts media as URL fields named by type.
                        # If the provider expects a different key, we still fall back to link-only text.
                        endpoint: normalized_url,
                        "filename": name,
                        "caption": "",
                    },
                    timeout=20,
                )
                data_media = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text}
                if str(data_media.get("sent")).lower() == "true" or str(data_media.get("status")).lower() == "success":
                    mid = str(data_media.get("id") or data_media.get("sid") or uuid.uuid4().hex[:10])
                    sent_media_ids.append(mid)
                else:
                    attachment_urls.append((name, original_url))
            except Exception:
                attachment_urls.append((name, original_url))
    else:
        for a in (content.get("attachments") or []):
            if isinstance(a, dict) and a.get("url"):
                attachment_urls.append((a.get("name") or "file", a["url"]))

    if attachment_urls:
        link_lines = [u for _, u in attachment_urls]
        body = (body or "").rstrip()
        suffix = "\n".join(link_lines)
        body = f"{body}\n\n{suffix}" if body else suffix

    response = http_requests.post(
        f"https://api.ultramsg.com/{instance_id}/messages/chat",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"token": token, "to": phone, "body": body},
        timeout=10,
    )
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}

    # UltraMsg may return sent=true even when delivery is pending; treat only this as enqueue accepted.
    if str(data.get("sent")).lower() == "true" or str(data.get("status")).lower() == "success":
        # UltraMsg webhook payloads typically include a stable SID/hash (and sometimes a different id)
        # than the immediate send response. Store these alongside the id so webhook matching works.
        sid = data.get("sid") or data.get("messageId") or data.get("message_id")
        msg_id = data.get("id") or sid or uuid.uuid4().hex[:10]
        msg_hash = data.get("hash")
        return {
            "provider": "ultramsg",
            # Prefer SID if present; webhook "data.sid" commonly matches this.
            # If we sent media items, keep the text message id as the primary but include all IDs for traceability.
            "provider_message_id": str(sid or msg_id),
            "provider_message_sid": (str(sid) if sid else None),
            "provider_message_hash": (str(msg_hash) if msg_hash else None),
            "provider_message_ids": ([str(x) for x in sent_media_ids] if sent_media_ids else None),
            "status": "SENT", "error": None,
        }
    raise RuntimeError(f"UltraMsg error: {data}")

def send_push(recipient_address: str, content: dict) -> dict:
    """
    Firebase Cloud Messaging (FCM) legacy HTTP API.
    .env keys needed:
      FCM_SERVER_KEY  — from Firebase Console → Project Settings → Cloud Messaging
    recipient_address is the device FCM token.
    """
    server_key = os.getenv("FCM_SERVER_KEY")
    if not server_key:
        raise ValueError("FCM_SERVER_KEY not set in .env")

    title = content.get("title", content.get("subject", "Notification"))
    body  = content.get("body", "")

    # Limits are enforced at the API layer (enforce_channel_limits in main.py)
    # before the job reaches this worker — title<=65, body<=240.
    # The [:65] and [:240] below are kept as a final safety net only.
    response = http_requests.post(
        "https://fcm.googleapis.com/fcm/send",
        headers={"Authorization": f"key={server_key}", "Content-Type": "application/json"},
        json={
            "to": recipient_address,
            "notification": {"title": title[:65], "body": body[:240]},
            "data": content,
        },
        timeout=10,
    )
    data = response.json()
    if response.status_code == 200 and data.get("success", 0) == 1:
        msg_id = data.get("results", [{}])[0].get("message_id", f"fcm_{uuid.uuid4().hex[:10]}")
        return {"provider": "firebase_fcm", "provider_message_id": msg_id, "status": "SENT", "error": None}

    error = data.get("results", [{}])[0].get("error", "Unknown FCM error")
    raise RuntimeError(f"FCM error: {error}")

# ── PROVIDER ROUTER ──────────────────────────────────────────────────────────
PROVIDER_FUNCTIONS = {
    "SendGrid": send_email,
    "SMTP": send_email_smtp,
    "SendFire": send_sms,
    "UltraMsg": send_whatsapp,
    "Firebase FCM": send_push,
}

def dispatch_with_failover(job):
    providers = job["providers_snapshot"]
    meta      = job.get("retry_meta", {})

    provider_index = meta.get("provider_index", 0)
    attempt        = meta.get("attempt", 1)

    if provider_index >= len(providers):
        raise RuntimeError("All providers exhausted")

    provider = providers[provider_index]
    provider_name = provider["provider_name"]
    max_retries   = provider.get("max_retries", 3)

    fn = PROVIDER_FUNCTIONS.get(provider_name)

    if not fn:
        print(f"[PROVIDER] Unsupported provider {provider_name}; skipping")
        job["retry_meta"]["provider_index"] = provider_index + 1
        job["retry_meta"]["attempt"] = 1
        if job["retry_meta"]["provider_index"] < len(providers):
            return None, "SWITCH_PROVIDER", RuntimeError(f"No function for provider {provider_name}")
        return None, "ALL_FAILED", RuntimeError(f"No function for provider {provider_name}")

    try:
        result = fn(job["recipient_address"], job.get("content") or {})
        result["provider"] = provider_name
        return result, None, None  # success

    except Exception as e:
        print(f"[PROVIDER] {provider_name} failed attempt {attempt}: {e}")

        # Retry same provider
        if attempt < max_retries:
            job["retry_meta"]["attempt"] = attempt + 1
            return None, "RETRY_SAME_PROVIDER", e

        # Switch provider
        job["retry_meta"]["provider_index"] = provider_index + 1
        job["retry_meta"]["attempt"] = 1

        if job["retry_meta"]["provider_index"] < len(providers):
            return None, "SWITCH_PROVIDER", e

        return None, "ALL_FAILED", e

def push_to_dlq(job, error_code, error_message):
    dlq_entry = {
        "job_id": job["job_id"],
        "notification_id": job.get("notification_id"),
        # Keep both channel + queue metadata so DLQ UI can display source queue.
        "channel": job.get("channel"),
        "queue_name": job.get("queue_name"),

        "recipient": {
            "email": job.get("recipient_address"),
            "user_id": (job.get("recipient") or {}).get("user_id") or job.get("user_id")
        },
        "payload": job.get("content"),

        "error_code": error_code,
        "error_type": classify_error(error_code),
        "error_message": error_message,

        "retry_count": job.get("retry_meta", {}).get("attempt", 1),
        "max_retries": job.get("max_retries", 5),

        "providers_tried": job.get("provider_history", []),
        "providers_snapshot": job.get("providers_snapshot", []),
        "failed_at": datetime.utcnow(),
        "created_at": job.get("created_at", datetime.utcnow()),

        "status": "DLQ",
        "next_retry_at": compute_next_retry(error_code)
    }
    _db["dlq"].insert_one(dlq_entry)

def log_failed_delivery(job: dict, error_message: str, *, status: str = "FAILED") -> None:
    """
    Record terminal per-recipient failures so dashboard counts match actual outcomes.
    One parent job can contain many recipients, so uniqueness is per job + recipient.
    """
    recipient = job.get("recipient") or {}
    content = job.get("content") or {}
    recipient_user_id = recipient.get("user_id") or content.get("user_id")
    recipient_address = job.get("recipient_address")
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    provider_index = (job.get("retry_meta") or {}).get("provider_index", 0)
    providers = job.get("providers_snapshot") or []
    provider = providers[provider_index] if provider_index < len(providers) else {}

    query = {
        "job_id": job.get("job_id"),
        "recipient_user_id": recipient_user_id,
        "recipient_address": recipient_address,
        "status": {"$in": ["FAILED", "DLQ", "DROPPED"]},
    }
    delivery_logs_collection.update_one(
        query,
        {
            "$setOnInsert": {
                "log_id": f"log_{uuid.uuid4().hex[:12]}",
                "job_id": job.get("job_id"),
                "request_id": job.get("request_id"),
                "client_id": job.get("client_id"),
                "recipient_user_id": recipient_user_id,
                "recipient_address": recipient_address,
                "event_type": job.get("event_type"),
                "channel": job.get("channel"),
                "provider": provider.get("provider_name"),
                "provider_priority": provider.get("priority"),
                "provider_id": provider.get("provider_id"),
                "provider_message_id": None,
                "provider_message_sid": None,
                "provider_message_hash": None,
                "attempt_number": (job.get("retry_meta") or {}).get("attempt", 1),
                "sent_at": None,
                "delivered_at": None,
                "read_at": None,
                "latency_ms": None,
                "created_at": now_ist,
            },
            "$set": {
                "status": status,
                "error": error_message,
                "failed_at": now_ist,
            },
        },
        upsert=True,
    )
    
def classify_error(error_code):
    PERMANENT_ERRORS = {
        "INVALID_EMAIL",
        "USER_UNSUBSCRIBED",
        "INVALID_PAYLOAD",
        "SPAM_REJECTED",
        "AUTH_FAILED"
    }

    if error_code in PERMANENT_ERRORS:
        return "PERMANENT"
    return "TRANSIENT"

def compute_next_retry(error_code):
    now = datetime.utcnow()

    if error_code == "RATE_LIMIT_EXCEEDED":
        return now + timedelta(minutes=30)

    if error_code == "SMTP_TIMEOUT":
        return now + timedelta(minutes=2)

    return now + timedelta(minutes=5)    

def map_exception_to_error_code(exc):
    msg = str(exc).lower()

    if "invalid" in msg:
        return "INVALID_EMAIL"

    if "timeout" in msg:
        return "SMTP_TIMEOUT"

    if "rate" in msg:
        return "RATE_LIMIT_EXCEEDED"

    if "auth" in msg:
        return "AUTH_FAILED"

    return "UNKNOWN_ERROR"

# ── Batch processing (1 Celery task per channel) ─────────────────────────────
def _requeue_single_job(job: dict, *, queue_name: str, countdown: int) -> None:
    """
    Re-enqueue a single-recipient job for later processing.
    This is used by channel-batch execution to avoid retrying the whole batch.
    """
    try:
        send_notification.apply_async(args=[job], queue=queue_name, countdown=max(int(countdown or 0), 0))
    except Exception as exc:
        print(f"[BATCH] Failed to requeue job {job.get('job_id')} to {queue_name}: {exc}")


def _process_single_job_in_batch(job: dict) -> None:
    """
    Process one single-recipient job *without* Celery self.retry (batch-safe).
    On transient states, requeues only this job.
    """
    job_id = job.get("job_id")
    if not job_id:
        return

    # Skip terminal states
    if job.get("status") in {"SENT", "DLQ", "DROPPED"}:
        return

    channel = str(job.get("channel") or "").strip().lower() or "email"
    queue_name = job.get("queue_name") or f"{channel}-notify-q"

    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    # Mark PROCESSING
    notification_jobs_collection.update_one(
        {"job_id": job_id, "status": {"$in": ["QUEUED", "PROCESSING"]}},
        {"$set": {"status": "PROCESSING", "updated_at": now_ist}},
    )

    try:
        job_content = dict(job.get("content") or {})
        job_content.setdefault("_job_id", job_id)
        job_content.setdefault("_request_id", job.get("request_id"))
        job_content.setdefault("_client_id", job.get("client_id"))
        job_content.setdefault("_channel", channel)
        job_content.setdefault("_recipient_user_id", (job.get("recipient") or {}).get("user_id"))
        job_content.setdefault("_recipient_address", job.get("recipient_address"))
        job["content"] = job_content
        job["channel"] = channel

        # ── Admin controls: pause + rate limit (per channel)
        control = _get_channel_control(channel)
        if control.get("paused"):
            print(f"[QUEUE_CONTROL] Channel '{channel}' paused — requeueing job {job_id}")
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"status": "QUEUED", "updated_at": now_ist}},
            )
            _requeue_single_job(job, queue_name=queue_name, countdown=5)
            return

        countdown = _enforce_channel_rate_limit(channel, control.get("rate_limit", 0))
        if countdown > 0:
            print(
                f"[QUEUE_CONTROL] Throttling channel '{channel}' "
                f"({control.get('rate_limit')} jobs/min) — delaying job {job_id} by {countdown}s"
            )
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"status": "QUEUED", "updated_at": now_ist}},
            )
            _requeue_single_job(job, queue_name=queue_name, countdown=countdown)
            return

        # Provider routing uses retry_meta to decide provider + attempt
        provider_index = (job.get("retry_meta") or {}).get("provider_index", 0)
        if provider_index >= len(job.get("providers_snapshot") or []):
            raise RuntimeError("Invalid provider index")

        provider = job["providers_snapshot"][provider_index]
        provider_name = provider["provider_name"]

        result, action, error = dispatch_with_failover(job)
        job.setdefault("provider_history", []).append(provider_name)

        # ───────────────── SUCCESS ─────────────────
        if result:
            sent_at = datetime.utcnow() + timedelta(hours=5, minutes=30)
            attempt = (job.get("retry_meta") or {}).get("attempt", 1)

            delivery_logs_collection.insert_one({
                "log_id":              f"log_{uuid.uuid4().hex[:12]}",
                "job_id":              job_id,
                "request_id":          job.get("request_id"),
                "client_id":           job.get("client_id"),
                "recipient_user_id":   (job.get("recipient") or {}).get("user_id") or (job.get("content") or {}).get("user_id"),
                "recipient_address":   job.get("recipient_address"),
                "event_type":          job.get("event_type"),
                "channel":             channel,
                "provider":            result["provider"],
                "provider_priority":   provider.get("priority"),
                "provider_id":         provider.get("provider_id"),
                "provider_message_id": result["provider_message_id"],
                "provider_message_sid":  result.get("provider_message_sid"),
                "provider_message_hash": result.get("provider_message_hash"),
                "status":              "SENT",
                "attempt_number":      attempt,
                "sent_at":             sent_at,
                "delivered_at":        None,
                "read_at":             None,
                "latency_ms":          None,
                "error":               None,
                "created_at":          sent_at,
            })

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "SENT",
                    "provider_history": job.get("provider_history", []),
                    "provider_message_id": result["provider_message_id"],
                    "updated_at": sent_at
                }},
            )

            _emit_client_webhook({
                "type": "NOTIFICATION_STATUS",
                "job_id": job_id,
                "request_id": job.get("request_id"),
                "client_id": job.get("client_id"),
                "event_type": job.get("event_type"),
                "channel": channel,
                "status": "SENT",
                "provider": result.get("provider"),
                "provider_message_id": result.get("provider_message_id"),
                "timestamp": sent_at.isoformat() if hasattr(sent_at, "isoformat") else str(sent_at),
            })
            return

        # ───────────── RETRY SAME PROVIDER ─────────────
        if action == "RETRY_SAME_PROVIDER":
            delay = min(10 * (2 ** (job["retry_meta"]["attempt"] - 1)), 300)
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "QUEUED",
                    "retry_meta": job["retry_meta"],
                    "provider_history": job.get("provider_history", []),
                    "updated_at": now_ist
                }},
            )
            _requeue_single_job(job, queue_name=queue_name, countdown=delay)
            return

        # ───────────── SWITCH PROVIDER ─────────────
        if action == "SWITCH_PROVIDER":
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "QUEUED",
                    "retry_meta": job["retry_meta"],
                    "provider_history": job.get("provider_history", []),
                    "updated_at": now_ist
                }},
            )
            _requeue_single_job(job, queue_name=queue_name, countdown=2)
            return

        # ───────────── ALL FAILED ─────────────
        if action == "ALL_FAILED":
            error_code = map_exception_to_error_code(error)
            error_message = str(error)
            existing = _db["dlq"].find_one({"job_id": job_id})
            if not existing:
                push_to_dlq(job, error_code, error_message)
            log_failed_delivery(job, error_message, status="FAILED")
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "DLQ",
                    "provider_history": job.get("provider_history", []),
                    "updated_at": now_ist
                }},
            )
            return

        raise RuntimeError("Unknown dispatch state")

    except Exception as exc:
        # Match single-job behavior: push to DLQ on fatal errors
        error_code = map_exception_to_error_code(exc)
        error_message = str(exc)
        existing = _db["dlq"].find_one({"job_id": job_id})
        if not existing:
            push_to_dlq(job, error_code, error_message)
        log_failed_delivery(job, error_message, status="FAILED")
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {"status": "DLQ", "updated_at": now_ist}},
        )
        print(f"[BATCH] Fatal error -> DLQ job={job_id}: {error_message}")


def _process_channel_batch(batch: dict) -> None:
    """
    Backward-compat: older payload shape used a batch that referenced existing
    single-recipient job documents by job_ids.
    Kept so any already-queued messages don't fail after deploy.
    """
    channel = str(batch.get("channel") or "").strip().lower() or "email"
    batch_id = batch.get("batch_id") or "batch"
    job_ids = batch.get("job_ids") or []
    print(f"[BATCH_COMPAT] Channel={channel} batch_id={batch_id} jobs={len(job_ids)}")

    for job_id in job_ids:
        try:
            doc = notification_jobs_collection.find_one({"job_id": job_id}, {"_id": 0})
            if not doc:
                continue
            _process_single_job_in_batch(doc)
        except Exception as exc:
            print(f"[BATCH_COMPAT] Error processing job {job_id}: {exc}")


# ════════════════════════════════════════════════════════════════════════════
#  CELERY TASK
# ════════════════════════════════════════════════════════════════════════════
@celery_app.task(bind=True, max_retries=5)
def send_notification(self, job: dict):

    # New mode: 1 job per channel that contains a recipients list.
    recipients_batch = job.get("recipients")
    if isinstance(recipients_batch, list):
        channel = str(job.get("channel") or "").strip().lower() or "email"
        job_id = job.get("job_id")
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

        print(f"\n{'─'*52}")
        print(f"[WORKER] PID {os.getpid()} | Channel job: {job_id}")
        print(f"         Channel:   {channel}")
        print(f"         Recipients:{len(recipients_batch)}")

        # Mark PROCESSING for the channel job
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {"status": "PROCESSING", "updated_at": now_ist}},
        )

        # Process each recipient payload as an isolated single job (no Celery self.retry on the batch)
        for item in recipients_batch:
            try:
                single = {
                    "job_id":            job_id,  # keep channel job_id as the parent id in logs
                    "request_id":        job.get("request_id"),
                    "client_id":         job.get("client_id"),
                    "event_type":        job.get("event_type"),
                    "recipient":         (item or {}).get("recipient") or {},
                    "channel":           channel,
                    "priority":          job.get("priority"),
                    "template_id":       job.get("template_id"),
                    "rendered_content":  job.get("rendered_content"),
                    "recipient_address": (item or {}).get("recipient_address"),
                    "queue_name":        job.get("queue_name"),
                    "queue_role":        job.get("queue_role"),
                    "content":           (item or {}).get("content") or {},
                    "providers_snapshot": job.get("providers_snapshot") or [],
                    "retry_meta":        (item or {}).get("retry_meta") or {"attempt": 1, "provider_index": 0},
                    "provider_history":  [],
                    "status":            "QUEUED",
                    "created_at":        job.get("created_at"),
                    "updated_at":        job.get("updated_at"),
                }
                _process_single_job_in_batch(single)
            except Exception as exc:
                print(f"[BATCH] Error processing recipient item in channel job {job_id}: {exc}")

        # Mark channel job as SENT if all recipient items are handled (per-recipient DLQ handled separately)
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {"status": "SENT", "updated_at": datetime.utcnow() + timedelta(hours=5, minutes=30)}},
        )
        return

    job_id            = job["job_id"]
    # Normalize channel to a canonical value so webhook updaters can match logs.
    channel           = str(job.get("channel") or "").strip().lower() or "email"
    job["channel"]    = channel
    recipient_address = job.get("recipient_address")
    retry_meta        = job.get("retry_meta", {})
    attempt           = retry_meta.get("attempt", 1)

    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    print(f"\n{'─'*52}")
    print(f"[WORKER] PID {os.getpid()} | Job: {job_id}")
    print(f"         Channel:   {channel}")
    print(f"         Recipient: {recipient_address}")
    print(f"         Attempt:   {attempt}")

    # Mark PROCESSING
    notification_jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {"status": "PROCESSING", "updated_at": now_ist}},
    )

    try:
        job_content = dict(job.get("content") or {})
        job_content.setdefault("_job_id", job_id)
        job_content.setdefault("_request_id", job.get("request_id"))
        job_content.setdefault("_client_id", job.get("client_id"))
        job_content.setdefault("_channel", channel)
        job_content.setdefault("_recipient_user_id", (job.get("recipient") or {}).get("user_id"))
        job_content.setdefault("_recipient_address", job.get("recipient_address"))
        job["content"] = job_content

        # ── Admin controls: pause + rate limit (per channel) ────────────────
        control = _get_channel_control(channel)
        if control.get("paused"):
            # Put the task back later (keeps the queue effectively "paused")
            print(f"[QUEUE_CONTROL] Channel '{channel}' is paused — requeueing job {job_id}")
            raise self.retry(countdown=5, args=[job])

        countdown = _enforce_channel_rate_limit(channel, control.get("rate_limit", 0))
        if countdown > 0:
            print(
                f"[QUEUE_CONTROL] Throttling channel '{channel}' "
                f"({control.get('rate_limit')} jobs/min) — delaying job {job_id} by {countdown}s"
            )
            raise self.retry(countdown=countdown, args=[job])

        # Safe provider access
        provider_index = job["retry_meta"].get("provider_index", 0)

        if provider_index >= len(job["providers_snapshot"]):
            raise RuntimeError("Invalid provider index")

        provider = job["providers_snapshot"][provider_index]
        provider_name = provider["provider_name"]

        # Call dispatcher
        result, action, error = dispatch_with_failover(job)

        # Track provider AFTER dispatch decision
        job.setdefault("provider_history", []).append(provider_name)

        # ───────────────── SUCCESS ─────────────────
        if result:
            sent_at = datetime.utcnow() + timedelta(hours=5, minutes=30)

            delivery_logs_collection.insert_one({
                "log_id":              f"log_{uuid.uuid4().hex[:12]}",
                "job_id":              job_id,
                "request_id":          job.get("request_id"),
                "client_id":           job.get("client_id"),
                "recipient_user_id":   (job.get("recipient") or {}).get("user_id") or (job.get("content") or {}).get("user_id"),
                "recipient_address":   job.get("recipient_address"),
                "event_type":          job.get("event_type"),
                "channel":             channel,
                "provider":            result["provider"],
                "provider_priority":   provider.get("priority"),
                "provider_id":         provider.get("provider_id"),
                "provider_message_id": result["provider_message_id"],
                # Optional extra identifiers for webhook correlation (e.g. UltraMsg uses sid/hash in callbacks)
                "provider_message_sid":  result.get("provider_message_sid"),
                "provider_message_hash": result.get("provider_message_hash"),
                "status":              "SENT",
                "attempt_number":      attempt,
                "sent_at":             sent_at,
                "delivered_at":        None,
                "read_at":             None,
                "latency_ms":          None,
                "error":               None,
                "created_at":          sent_at,
            })

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "SENT",
                    "provider_history": job["provider_history"],
                    "provider_message_id": result["provider_message_id"],
                    "updated_at": sent_at
                }},
            )

            print(f"[WORKER] ✓ SENT via {result['provider']}")

            # Client webhook (best-effort) — useful for WhatsApp tracking as well.
            _emit_client_webhook({
                "type": "NOTIFICATION_STATUS",
                "job_id": job_id,
                "request_id": job.get("request_id"),
                "client_id": job.get("client_id"),
                "event_type": job.get("event_type"),
                "channel": channel,
                "status": "SENT",
                "provider": result.get("provider"),
                "provider_message_id": result.get("provider_message_id"),
                "timestamp": sent_at.isoformat() if hasattr(sent_at, "isoformat") else str(sent_at),
            })
            return

        # ───────────── RETRY SAME PROVIDER ─────────────
        elif action == "RETRY_SAME_PROVIDER":
            delay = min(10 * (2 ** (job["retry_meta"]["attempt"] - 1)), 300)

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "retry_meta": job["retry_meta"],
                    "provider_history": job["provider_history"],
                    "updated_at": now_ist
                }},
            )

            raise self.retry(countdown=delay, args=[job])

        # ───────────── SWITCH PROVIDER ─────────────
        elif action == "SWITCH_PROVIDER":
            print("[WORKER] Switching provider...")

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "retry_meta": job["retry_meta"],
                    "provider_history": job["provider_history"],
                    "updated_at": now_ist
                }},
            )

            raise self.retry(countdown=2, args=[job])

        # ───────────── ALL FAILED ─────────────
        elif action == "ALL_FAILED":
            error_code = map_exception_to_error_code(error)
            error_message = str(error)
            existing = _db["dlq"].find_one({"job_id": job_id})
            if not existing:
                push_to_dlq(job, error_code, error_message)
            log_failed_delivery(job, error_message, status="FAILED")
            
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "DLQ",
                    "provider_history": job["provider_history"],
                    "updated_at": now_ist
                }},
            )

            print("[WORKER] All providers failed → pushed to DLQ")
            return

        # ───────────── SAFETY FALLBACK ─────────────
        else:
            raise RuntimeError("Unknown dispatch state")

    except Exception as exc:
        if job.get("status") != "DLQ":
            error_code = map_exception_to_error_code(exc)
            error_message = str(exc)
            existing = _db["dlq"].find_one({"job_id": job["job_id"]})
            if not existing:
                push_to_dlq(job, error_code, error_message)
            log_failed_delivery(job, error_message, status="FAILED")

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "DLQ",
                    "updated_at": now_ist
                }},
            )
        print(f"[WORKER] Fatal error -> DLQ: {error_message}")
        raise exc
