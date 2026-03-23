from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
import os
import uuid
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from celery_config import celery_app
from tasks import send_notification

load_dotenv()

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
        # No preferences stored — honour caller's requested channels
        return channels_requested or []

    # Global DND check
    global_dnd = preference.get("dnd_windows", [])
    if global_dnd and is_in_dnd(global_dnd):
        return []

    # Per-event preferences
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
        # Event type not in user prefs — fall back to requested channels
        return channels_requested or []

    if not channels_requested:
        return user_allowed

    # Intersect: only send on channels both caller AND user want
    return [ch for ch in channels_requested if ch in user_allowed]


# ── CHANNEL → RECIPIENT ADDRESS MAPPING ──────────────────────────────────────

CHANNEL_ADDRESS_MAP = {
    "email":    lambda r: r.get("email"),
    "sms":      lambda r: r.get("phone"),
    "whatsapp": lambda r: r.get("wa_number"),
    "push":     lambda r: r.get("fcm_token"),
}


# ── PREFERENCES ROUTES ────────────────────────────────────────────────────────
# NOTE: user_id is hardcoded as a placeholder below.
# When the login team delivers the auth token, replace PLACEHOLDER_USER_ID
# with the decoded user_id from the token header.

PLACEHOLDER_USER_ID = "placeholder_user"  # TODO: replace with token decode


@app.post("/preferences")
def set_preferences(pref: UserPreferencesRequest):
    user_id = PLACEHOLDER_USER_ID
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


@app.get("/preferences")
def get_preferences():
    user_id = PLACEHOLDER_USER_ID
    pref = preferences_collection.find_one({"user_id": user_id}, {"_id": 0})
    if not pref:
        raise HTTPException(status_code=404, detail="No preferences found.")
    return pref


# ── NOTIFICATION ROUTE ────────────────────────────────────────────────────────

@app.post("/notify")
def notify(request: NotificationRequest):
    """
    Flow:
      1. Store raw incoming request in notification_requests (no logic)
      2. For each recipient:
           a. resolve_channels() — DND check + user preference intersection
           b. For each resolved channel — create one notification_jobs doc
           c. Dispatch that job to the correct RabbitMQ queue via Celery
      3. Update notification_requests status
      4. Return summary
    """
    now_ist    = datetime.utcnow() + timedelta(hours=5, minutes=30)
    request_id = f"req_{uuid.uuid4().hex[:12]}"

    # Step 1 — Store raw request
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
    channel_jobs: dict[str, list[str]] = {}  # for grouped terminal output

    # Step 2 — Per recipient, per channel: create job + dispatch to Celery
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
            job_id = f"job_{uuid.uuid4().hex[:12]}"

            recipient_address = CHANNEL_ADDRESS_MAP.get(channel, lambda r: None)(recipient_dict)

            job_doc = {
                "job_id":           job_id,
                "request_id":       request_id,
                "client_id":        request.client_id,
                "event_type":       request.event_type,

                # Single recipient — split from the recipients array
                "recipient": {
                    "user_id":   recipient.user_id,
                    "email":     recipient.email,
                    "phone":     recipient.phone,
                    "wa_number": recipient.wa_number,
                    "fcm_token": recipient.fcm_token,
                },

                "channel":           channel,
                "priority":          request.priority or "MEDIUM",
                "template_id":       None,       # resolved by template engine (future sprint)
                "rendered_content":  None,       # rendered by template engine (future sprint)
                "recipient_address": recipient_address,

                "retry_policy": {
                    "max_attempts":    5,
                    "current_attempt": 1,
                    "backoff_seconds": [10, 30, 120, 600, 1800],
                },

                "queue_name": f"{channel}-notify-q",
                "content":    request.content,
                "status":     "QUEUED",
                "created_at": now_ist,
                "updated_at": now_ist,
            }

            # Step 2a — Persist job to MongoDB
            notification_jobs_collection.insert_one(job_doc)

            # Step 2b — Remove non-serialisable MongoDB _id before sending to Celery
            job_doc.pop("_id", None)
            # Convert datetime objects to ISO strings for Celery serialisation
            job_doc["created_at"] = job_doc["created_at"].isoformat()
            job_doc["updated_at"] = job_doc["updated_at"].isoformat()

            # Step 2c — Dispatch to the correct RabbitMQ queue via Celery
            send_notification.apply_async(
                args=[job_doc],
                queue=job_doc["queue_name"],
            )

            jobs_created.append(job_id)
            channel_jobs.setdefault(channel, []).append(job_id)

    # Grouped terminal output — one line per channel
    for channel, job_ids in channel_jobs.items():
        print(f"[QUEUE] {channel}-notify-q  <- {len(job_ids)} job(s): {', '.join(job_ids)}")

    # Step 3 — Update request status
    final_status = "JOBS_CREATED" if jobs_created else "FAILED"
    notification_requests_collection.update_one(
        {"request_id": request_id},
        {"$set": {"status": final_status}},
    )

    return {
        "message":      "Notification request processed",
        "request_id":   request_id,
        "jobs_created": jobs_created,
        "jobs_blocked": jobs_blocked,
    }


# ── USER EVENT TYPES ROUTE ────────────────────────────────────────────────────

@app.get("/user-event-types/{user_id}")
def get_user_event_types(user_id: str):
    """
    Returns distinct event_types from notification_requests
    where this user_id appears in the recipients array.
    Frontend calls this on load to populate the event-type dropdown.
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