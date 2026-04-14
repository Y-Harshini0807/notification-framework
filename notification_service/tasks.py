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
from email.mime.text import MIMEText

load_dotenv()

# ── DB connection inside worker process ──────────────────────────────────────
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
_mongo_client = MongoClient(MONGO_URL)
_db = _mongo_client["notification_db"]
notification_jobs_collection = _db["notification_jobs"]
delivery_logs_collection     = _db["delivery_logs"]

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

    response = http_requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": recipient_address}], "subject": subject}],
            "from":    {"email": from_email},
            "content": [
                {"type": "text/plain", "value": body_text},
                {"type": "text/html",  "value": body_html},
            ],
        },
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

    body  = content.get("body", content.get("subject", "Notification"))
    phone = recipient_address.lstrip("+")   # UltraMsg wants no leading +

    response = http_requests.post(
        f"https://api.ultramsg.com/{instance_id}/messages/chat",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"token": token, "to": phone, "body": body},
        timeout=10,
    )
    data = response.json()
    if str(data.get("sent")).lower() == "true":
        return {
            "provider": "ultramsg",
            "provider_message_id": str(data.get("id", uuid.uuid4().hex[:10])),
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
        raise RuntimeError(f"No function for provider {provider_name}")

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
            "user_id": job.get("user_id")
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

# ════════════════════════════════════════════════════════════════════════════
#  CELERY TASK
# ════════════════════════════════════════════════════════════════════════════
@celery_app.task(bind=True, max_retries=5)
def send_notification(self, job: dict):

    job_id            = job["job_id"]
    channel           = job["channel"]
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
                "event_type":          job.get("event_type"),
                "channel":             channel,
                "provider":            result["provider"],
                "provider_priority":   provider.get("priority"),
                "provider_id":         provider.get("provider_id"),
                "provider_message_id": result["provider_message_id"],
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
                    "updated_at": sent_at
                }},
            )

            print(f"[WORKER] ✓ SENT via {result['provider']}")
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

            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {
                    "status": "DLQ",
                    "updated_at": now_ist
                }},
            )
        print(f"[WORKER] Fatal error -> DLQ: {error_message}")
        raise exc