"""
Celery worker — consumes notification jobs from RabbitMQ queues,
calls the real provider for each channel, writes delivery_logs to MongoDB.

Start workers (from project root, inside venv):
  celery -A tasks worker --loglevel=info --concurrency=4 \
    -Q email-notify-q,sms-notify-q,whatsapp-notify-q,push-notify-q

Provider credentials go in .env — see .env.example for all keys.
To test only ONE channel, fill in just that channel's keys and leave the rest blank.
"""

from celery_config import celery_app
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests as http_requests
import uuid
import os

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
    
    body_html = content.get("html_body") or f"<p>{body_text}</p>"

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


def send_sms(recipient_address: str, content: dict) -> dict:
    """
    SendFire SMS API.
    .env keys needed:
      SENDFIRE_API_KEY    — your SendFire API key
      SENDFIRE_SENDER_ID  — alphanumeric sender ID e.g. NOTIFY
    SMS body is auto-truncated to 160 chars.
    """
    api_key   = os.getenv("SENDFIRE_API_KEY")
    sender_id = os.getenv("SENDFIRE_SENDER_ID", "NOTIFY")
    if not api_key:
        raise ValueError("SENDFIRE_API_KEY not set in .env")

    body = content.get("body", content.get("subject", "Notification"))
    if len(body) > 160:
        body = body[:157] + "..."

    response = http_requests.post(
        "https://api.sendfire.co/sms/send",
        json={"api_key": api_key, "sender_id": sender_id, "to": recipient_address, "message": body},
        timeout=10,
    )
    data = response.json()
    if response.status_code == 200 and data.get("status") == "success":
        return {
            "provider": "sendfire",
            "provider_message_id": str(data.get("message_id", uuid.uuid4().hex[:10])),
            "status": "SENT", "error": None,
        }
    raise RuntimeError(f"SendFire {response.status_code}: {data}")


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

PROVIDER_NAMES = {
    "email":    "sendgrid",
    "sms":      "sendfire",
    "whatsapp": "ultramsg",
    "push":     "firebase_fcm",
}

PROVIDER_MAP = {
    "email":    send_email,
    "sms":      send_sms,
    "whatsapp": send_whatsapp,
    "push":     send_push,
}

def dispatch_to_provider(channel: str, recipient_address: str, content: dict) -> dict:
    fn = PROVIDER_MAP.get(channel)
    if not fn:
        raise ValueError(f"Unknown channel: {channel}")
    return fn(recipient_address, content or {})


# ════════════════════════════════════════════════════════════════════════════
#  CELERY TASK
# ════════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, max_retries=5)
def send_notification(self, job: dict):
    """
    Consumes one job_doc from a RabbitMQ channel queue.
    Dispatches to the real provider, writes delivery_log, updates job status.
    """
    job_id            = job["job_id"]
    channel           = job["channel"]
    recipient_address = job.get("recipient_address")
    attempt           = job.get("retry_policy", {}).get("current_attempt", 1)
    max_attempts      = job.get("retry_policy", {}).get("max_attempts", 5)
    now_ist           = datetime.utcnow() + timedelta(hours=5, minutes=30)

    print(f"\n{'─'*52}")
    print(f"[WORKER] PID {os.getpid()} | Job: {job_id}")
    print(f"         Channel:   {channel}")
    print(f"         Recipient: {recipient_address}")
    print(f"         Attempt:   {attempt}/{max_attempts}")

    # Mark PROCESSING
    notification_jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {"status": "PROCESSING", "updated_at": now_ist}},
    )

    try:
        # ── Real provider dispatch ───────────────────────────────────────
        result  = dispatch_to_provider(channel, recipient_address, job.get("content") or {})
        sent_at = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # ── Write SENT delivery log ──────────────────────────────────────
        delivery_logs_collection.insert_one({
            "log_id":              f"log_{uuid.uuid4().hex[:12]}",
            "job_id":              job_id,
            "event_type":          job.get("event_type"),
            "channel":             channel,
            "provider":            result["provider"],
            "provider_message_id": result["provider_message_id"],
            "status":              "SENT",
            "attempt_number":      attempt,
            "sent_at":             sent_at,
            "delivered_at":        None,   # filled by provider webhook (future sprint)
            "read_at":             None,   # filled by open-pixel / push ACK (future sprint)
            "latency_ms":          None,
            "error":               None,
            "created_at":          sent_at,
        })

        # ── Mark job SENT ────────────────────────────────────────────────
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {"status": "SENT", "updated_at": sent_at}},
        )

        print(f"[WORKER] ✓ SENT via {result['provider']} | msg_id: {result['provider_message_id']}")

    except Exception as exc:
        # ── Retry with exponential backoff ───────────────────────────────
        backoff     = job.get("retry_policy", {}).get("backoff_seconds", [10, 30, 120, 600, 1800])
        countdown   = backoff[min(attempt - 1, len(backoff) - 1)]
        failed_at   = datetime.utcnow() + timedelta(hours=5, minutes=30)

        print(f"[WORKER] ✗ FAILED (attempt {attempt}/{max_attempts}): {exc}")

        # Update job status + attempt counter
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {
                "status":                       "FAILED",
                "retry_policy.current_attempt": attempt + 1,
                "updated_at":                   failed_at,
            }},
        )

        # Write FAILED delivery log for this attempt
        delivery_logs_collection.insert_one({
            "log_id":              f"log_{uuid.uuid4().hex[:12]}",
            "job_id":              job_id,
            "event_type":          job.get("event_type"),
            "channel":             channel,
            "provider":            PROVIDER_NAMES.get(channel, "unknown"),
            "provider_message_id": None,
            "status":              "FAILED",
            "attempt_number":      attempt,
            "sent_at":             None,
            "delivered_at":        None,
            "read_at":             None,
            "latency_ms":          None,
            "error":               {"message": str(exc)},
            "created_at":          failed_at,
        })

        if attempt >= max_attempts:
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"status": "DLQ"}},
            )
            print(f"[WORKER] ✗ Max retries reached — job {job_id} moved to DLQ")
            return

        job["retry_policy"]["current_attempt"] = attempt + 1
        print(f"[WORKER]   Retrying in {countdown}s ...")
        raise self.retry(exc=exc, countdown=countdown, args=[job])
