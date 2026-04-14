from fastapi import FastAPI, HTTPException, Request, Depends, Header, Response
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
from collections import defaultdict
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from celery_config import celery_app
from tasks import send_notification
from dlq_processor import run_worker

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
        "max_requests":   100,    # max requests per IP in the window
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


# ── RATE LIMIT CONFIGURATION ──────────────────────────────────────────────────
# All windows are rolling (checked against now - window_seconds).

RATE_LIMITS = {
    "email": {
        "per_user":   {"max": 5,   "window_seconds": 60},      # 5 emails/user/minute
        "per_client": {"max": 1000, "window_seconds": 3600},   # 1000 emails/client/hour
    },
    "sms": {
        "per_user":   {"max": 5,    "window_seconds": 3600},
        "per_client": {"max": 500,  "window_seconds": 3600},
    },
    "whatsapp": {
        "per_user":   {"max": 5,    "window_seconds": 3600},
        "per_client": {"max": 500,  "window_seconds": 3600},
    },
    "push": {
        "per_user":   {"max": 50,   "window_seconds": 3600},
        "per_client": {"max": 5000, "window_seconds": 3600},
    },
}


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

    # ── 1. Per-user check ────────────────────────────────────────────────────
    user_cfg    = limits["per_user"]
    user_window = timedelta(seconds=user_cfg["window_seconds"])
    user_since  = now_utc - user_window

    user_count = notification_jobs_collection.count_documents({
        "channel":           channel,
        "recipient.user_id": user_id,
        "created_at":        {"$gte": user_since},
        "status":            {"$in": ["QUEUED", "PROCESSING", "SENT", "DELIVERED", "READ"]},
    })

    if user_count >= user_cfg["max"]:
        oldest = notification_jobs_collection.find_one(
            {
                "channel":           channel,
                "recipient.user_id": user_id,
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
                            user_count, user_cfg["max"], now_ist)
        return {
            "allowed":             False,
            "scope":               "user",
            "count":               user_count,
            "limit":               user_cfg["max"],
            "window_seconds":      user_cfg["window_seconds"],
            "retry_after_seconds": retry_after,
            "channel":             channel,
        }

    # ── 2. Per-client check ──────────────────────────────────────────────────
    client_cfg    = limits["per_client"]
    client_window = timedelta(seconds=client_cfg["window_seconds"])
    client_since  = now_utc - client_window

    client_count = notification_jobs_collection.count_documents({
        "channel":    channel,
        "client_id":  client_id,
        "created_at": {"$gte": client_since},
        "status":     {"$in": ["QUEUED", "PROCESSING", "SENT", "DELIVERED", "READ"]},
    })

    if client_count >= client_cfg["max"]:
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
                            client_count, client_cfg["max"], now_ist)
        return {
            "allowed":             False,
            "scope":               "client",
            "count":               client_count,
            "limit":               client_cfg["max"],
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
    monthly_quota = body.get("monthly_quota", 100000)
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
    monthly_quota = int(body.get("monthly_quota", 100000))
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

    if db_providers:
        return [
            {
                "provider_id":   p.get("provider_id"),
                "provider_name": p.get("provider_name"),
                "priority":      p.get("priority"),
                "max_retries":   p.get("max_retries", 3),
                "timeout_ms":    p.get("timeout_ms", 5000),
            }
            for p in db_providers
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
    monthly_quota = api_client.get("monthly_quota", 100000)
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
    channel_jobs: dict[str, list[str]] = {}

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
            channel_content = {
                **(request.content or {}),
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

            job_id            = f"job_{uuid.uuid4().hex[:12]}"
            recipient_address = CHANNEL_ADDRESS_MAP.get(channel, lambda r: None)(recipient_dict)

            job_doc = {
                "job_id":           job_id,
                "request_id":       request_id,
                "client_id":        request.client_id,
                "event_type":       request.event_type,
                "recipient": {
                    "user_id":   recipient.user_id,
                    "email":     recipient.email,
                    "phone":     recipient.phone,
                    "wa_number": recipient.wa_number,
                    "fcm_token": recipient.fcm_token,
                },
                "channel":           channel,
                "priority":          request.priority or "MEDIUM",
                "template_id":       None,
                "rendered_content":  None,
                "recipient_address": recipient_address,
                "retry_policy": {
                    "max_attempts":    5,
                    "current_attempt": 1,
                    "backoff_seconds": [10, 30, 120, 600, 1800],
                },
                "queue_name": f"{channel}-notify-q",
                # Use validated_content — already has user_id, event_type injected,
                # and for SMS includes sms_segments if body was split.
                "content": validated_content,
                # providers_snapshot — ordered list of providers to try for this channel.
                # tasks.py dispatch_with_failover() iterates through these on failure.
                # Email has SendGrid as primary, SMTP as fallback.
                "providers_snapshot": _build_providers_snapshot(channel),
                "retry_meta": {
                    "attempt":        1,
                    "provider_index": 0,
                },
                "status":     "QUEUED",
                # IMPORTANT: store as UTC so check_rate_limit() window queries work correctly.
                # check_rate_limit uses datetime.utcnow() for $gte — must match storage clock.
                "created_at": now_utc,
                "updated_at": now_utc,
            }

            notification_jobs_collection.insert_one(job_doc)
            job_doc.pop("_id", None)
            job_doc["created_at"] = job_doc["created_at"].isoformat()
            job_doc["updated_at"] = job_doc["updated_at"].isoformat()

            send_notification.apply_async(
                args=[job_doc],
                queue=job_doc["queue_name"],
            )

            jobs_created.append(job_id)
            channel_jobs.setdefault(channel, []).append(job_id)

    for channel, job_ids in channel_jobs.items():
        print(f"[QUEUE] {channel}-notify-q  <- {len(job_ids)} job(s): {', '.join(job_ids)}")

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


@app.get("/jobs")
def get_jobs(
    client_id: Optional[str] = None,
    channel: Optional[str] = None,
    status: Optional[str] = None,
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
            },
        )
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    return {"total": total, "skip": skip, "limit": limit, "count": len(docs), "jobs": docs}


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
    if date_filter:
        match["created_at"] = date_filter

    # user_id and client_id live on the joined job document
    if user_id:   match["_job.recipient.user_id"] = user_id
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
            # Nest the enriched job fields under a "job" key
            "job": {
                "job_id":            "$_job.job_id",
                "client_id":         "$_job.client_id",
                "recipient_user_id": "$_job.recipient.user_id",
                "recipient_address": "$_job.recipient_address",
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
        provider_msg_id = raw_msg_id.split(".")[0] if raw_msg_id else None
        event_ts        = event.get("timestamp")
        now_ist         = datetime.utcnow() + timedelta(hours=5, minutes=30)

        if not provider_msg_id:
            skipped += 1
            continue

        event_time = (
            datetime.utcfromtimestamp(event_ts) + timedelta(hours=5, minutes=30)
            if event_ts else now_ist
        )

        if sg_event == "delivered":
            log = delivery_logs_collection.find_one({"provider_message_id": provider_msg_id})

            latency_ms = None
            if log and log.get("sent_at"):
                sent_at = log["sent_at"]
                if hasattr(sent_at, "tzinfo") and sent_at.tzinfo is not None:
                    sent_at = sent_at.replace(tzinfo=None)
                delta      = event_time - sent_at
                latency_ms = max(0, int(delta.total_seconds() * 1000))

            delivery_logs_collection.update_one(
                {"provider_message_id": provider_msg_id},
                {"$set": {
                    "status":       "DELIVERED",
                    "delivered_at": event_time,
                    "latency_ms":   latency_ms,
                }},
            )

            if log:
                notification_jobs_collection.update_one(
                    {"job_id": log["job_id"]},
                    {"$set": {"status": "DELIVERED"}},
                )
            processed += 1

        elif sg_event == "open":
            delivery_logs_collection.update_one(
                {"provider_message_id": provider_msg_id},
                {"$set": {"status": "READ", "read_at": event_time}},
            )
            processed += 1

        elif sg_event == "bounce":
            bounce_reason = event.get("reason", "Bounced")
            delivery_logs_collection.update_one(
                {"provider_message_id": provider_msg_id},
                {"$set": {
                    "status": "FAILED",
                    "error":  {"message": f"Bounce: {bounce_reason}"},
                }},
            )
            log = delivery_logs_collection.find_one({"provider_message_id": provider_msg_id})
            if log:
                notification_jobs_collection.update_one(
                    {"job_id": log["job_id"]},
                    {"$set": {"status": "FAILED"}},
                )
            processed += 1

        elif sg_event == "spamreport":
            delivery_logs_collection.update_one(
                {"provider_message_id": provider_msg_id},
                {"$set": {"status": "SPAM"}},
            )
            processed += 1

        else:
            skipped += 1

    print(f"[WEBHOOK] SendGrid: {len(events)} event(s) — {processed} processed, {skipped} skipped")

    return {"received": len(events), "processed": processed, "skipped": skipped}