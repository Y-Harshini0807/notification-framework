"""
Start workers:
  celery -A tasks worker --loglevel=info --concurrency=4 \
    -Q email-notify-q,sms-notify-q,whatsapp-notify-q,push-notify-q
"""

from celery_config import celery_app
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import uuid
import os

# ── DB connection inside worker process ──────────────────────────────────────
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
_mongo_client = MongoClient(MONGO_URL)
_db = _mongo_client["notification_db"]
notification_jobs_collection = _db["notification_jobs"]
delivery_logs_collection     = _db["delivery_logs"]


# ── SIMULATED PROVIDER DISPATCH ──────────────────────────────────────────────

def simulate_provider_send(channel: str, recipient_address: str, content: dict | None) -> dict:
    """
    Placeholder for real provider calls (SendGrid / Twilio / FCM / UltraMsg).
    Returns a dict with provider_message_id and status.
    Replace the body of each branch with the actual SDK call in future sprints.
    """
    time.sleep(2)  # simulate network latency

    provider_map = {
        "email":    "sendgrid",
        "sms":      "twilio",
        "whatsapp": "ultramsg",
        "push":     "firebase_fcm",
    }

    return {
        "provider":           provider_map.get(channel, "unknown"),
        "provider_message_id": f"msg_{uuid.uuid4().hex[:10]}",
        "status":             "SENT",
        "error":              None,
    }


# ── CELERY TASK ───────────────────────────────────────────────────────────────

@celery_app.task(bind=True, max_retries=5)
def send_notification(self, job: dict):
    """
    Consumes one job_doc from a RabbitMQ channel queue.

    job_doc keys used here:
      job_id, channel, recipient_address, recipient, event_type,
      content, retry_policy, queue_name
    """
    job_id           = job["job_id"]
    channel          = job["channel"]
    recipient_address = job.get("recipient_address")
    attempt          = job.get("retry_policy", {}).get("current_attempt", 1)

    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    print(f"\n{'─'*50}")
    print(f"[WORKER] PID {os.getpid()} | Job: {job_id}")
    print(f"         Channel:   {channel}")
    print(f"         Recipient: {recipient_address}")
    print(f"         Attempt:   {attempt}")

    # Mark job as PROCESSING in MongoDB
    notification_jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {"status": "PROCESSING", "updated_at": now_ist}},
    )

    try:
        # ── Provider dispatch (simulated) ────────────────────────────────
        result = simulate_provider_send(channel, recipient_address, job.get("content"))

        sent_at = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # ── Write delivery log ───────────────────────────────────────────
        log_doc = {
            "log_id":              f"log_{uuid.uuid4().hex[:12]}",
            "job_id":              job_id,
            "event_type":          job.get("event_type"),
            "channel":             channel,
            "provider":            result["provider"],
            "provider_message_id": result["provider_message_id"],
            "status":              result["status"],          # SENT | FAILED
            "attempt_number":      attempt,
            "sent_at":             sent_at,
            "delivered_at":        None,   # updated by provider webhook (future)
            "read_at":             None,   # updated by open-pixel / push ACK (future)
            "latency_ms":          None,
            "error":               result.get("error"),
            "created_at":          sent_at,
        }
        delivery_logs_collection.insert_one(log_doc)

        # ── Update job status to SENT ────────────────────────────────────
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {"status": "SENT", "updated_at": sent_at}},
        )

        print(f"[WORKER] ✓ Sent via {result['provider']} | msg_id: {result['provider_message_id']}")

    except Exception as exc:
        # ── Retry with exponential backoff ───────────────────────────────
        backoff = job.get("retry_policy", {}).get("backoff_seconds", [10, 30, 120, 600, 1800])
        retry_index = min(attempt - 1, len(backoff) - 1)
        countdown   = backoff[retry_index]

        print(f"[WORKER] ✗ Failed (attempt {attempt}): {exc}. Retrying in {countdown}s ...")

        # Increment attempt counter in job_doc before re-queuing
        job["retry_policy"]["current_attempt"] = attempt + 1

        # Update MongoDB attempt count and status
        notification_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {
                "status":                          "FAILED",
                "retry_policy.current_attempt":    attempt + 1,
                "updated_at":                      datetime.utcnow() + timedelta(hours=5, minutes=30),
            }},
        )

        max_attempts = job.get("retry_policy", {}).get("max_attempts", 5)
        if attempt >= max_attempts:
            # Move to DLQ status — admin must resolve manually
            notification_jobs_collection.update_one(
                {"job_id": job_id},
                {"$set": {"status": "DLQ"}},
            )
            print(f"[WORKER] ✗ Max retries reached for {job_id} — moved to DLQ")
            return  # do not retry further

        raise self.retry(exc=exc, countdown=countdown, args=[job])