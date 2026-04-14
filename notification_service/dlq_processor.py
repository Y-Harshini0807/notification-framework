"""
DLQ Processor — polls the `dlq` collection and requeues recoverable jobs.

Runs alongside Celery worker:
  python dlq_processor.py

Logic:
  - PERMANENT errors (invalid email, unsubscribed, auth failed) → DROPPED immediately
  - RATE_LIMIT_EXCEEDED → requeued with delay (respects next_retry_at)
  - SMTP_TIMEOUT → requeued with provider switch
  - All others → requeued normally
  - Jobs requeued 3+ times → DROPPED
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import os

load_dotenv()

from tasks import send_notification

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client    = MongoClient(MONGO_URL)
db        = client["notification_db"]

dlq_collection  = db["dlq"]
jobs_collection = db["notification_jobs"]


# ── CHANNEL → recipient address field mapping ─────────────────────────────────
CHANNEL_ADDRESS_MAP = {
    "email":    lambda r: r.get("email"),
    "sms":      lambda r: r.get("phone"),
    "whatsapp": lambda r: r.get("wa_number"),
    "push":     lambda r: r.get("fcm_token"),
}


def process_dlq():
    now = datetime.utcnow()

    # Pick one DLQ job whose next_retry_at has passed
    job = dlq_collection.find_one_and_update(
        {"status": "DLQ", "next_retry_at": {"$lte": now}},
        {"$set": {"status": "PROCESSING"}},
    )
    if not job:
        return  # nothing ready yet

    handle_job(job)


def handle_job(job):
    # Max requeue attempts exceeded → drop
    if job.get("requeue_count", 0) >= 3:
        mark_dropped(job, "Max requeue attempts reached")
        return

    # Permanent errors → drop immediately, no point retrying
    if job.get("error_type") == "PERMANENT":
        mark_dropped(job, f"Permanent error: {job.get('error_code')}")
        return

    error_code = job.get("error_code", "UNKNOWN_ERROR")

    if error_code == "RATE_LIMIT_EXCEEDED":
        requeue(job, delay=True)
    elif error_code == "SMTP_TIMEOUT":
        requeue(job, switch_provider=True)
    else:
        requeue(job)


def requeue(job, delay=False, switch_provider=False):
    job_id = job["job_id"]

    # ── Get original job from notification_jobs for full context ─────────────
    original = jobs_collection.find_one({"job_id": job_id})
    if not original:
        mark_dropped(job, "Original job not found in notification_jobs")
        return

    channel            = original.get("channel", "email")
    providers_snapshot = original.get("providers_snapshot", [])
    queue_name         = original.get("queue_name", f"{channel}-notify-q")

    # ── Validate providers_snapshot ──────────────────────────────────────────
    if not providers_snapshot:
        mark_dropped(job, "providers_snapshot missing — cannot requeue")
        return

    # ── Resolve recipient address from original job's recipient dict ─────────
    recipient_dict    = original.get("recipient", {})
    addr_fn           = CHANNEL_ADDRESS_MAP.get(channel, lambda r: r.get("email"))
    recipient_address = addr_fn(recipient_dict)

    if not recipient_address:
        mark_dropped(job, f"No recipient address for channel '{channel}'")
        return

    # ── Determine provider index ─────────────────────────────────────────────
    current_index = original.get("retry_meta", {}).get("provider_index", 0)
    if switch_provider:
        current_index = min(current_index + 1, len(providers_snapshot) - 1)

    # ── Build the requeued job doc ────────────────────────────────────────────
    new_job = {
        "job_id":            job_id,
        "notification_id":   job.get("notification_id"),
        "channel":           channel,
        "queue_name":        queue_name,
        "event_type":        original.get("event_type"),
        "client_id":         original.get("client_id"),
        "recipient":         recipient_dict,
        "recipient_address": recipient_address,
        "content":           job.get("payload") or original.get("content", {}),
        "providers_snapshot": providers_snapshot,
        "retry_meta": {
            "attempt":        1,
            "provider_index": current_index,
        },
        "provider_history":  job.get("providers_tried", []),
        "created_at":        original.get("created_at", datetime.utcnow()),
    }

    # ── Compute countdown ────────────────────────────────────────────────────
    countdown = 0
    if delay:
        next_retry = job.get("next_retry_at")
        if next_retry:
            # next_retry_at may be timezone-aware from MongoDB
            if hasattr(next_retry, "tzinfo") and next_retry.tzinfo is not None:
                next_retry = next_retry.replace(tzinfo=None)
            delta     = (next_retry - datetime.utcnow()).total_seconds()
            countdown = max(int(delta), 0)
        else:
            countdown = 1800  # default 30 min

    # ── Dispatch to Celery ───────────────────────────────────────────────────
    send_notification.apply_async(args=[new_job], queue=queue_name, countdown=countdown)

    # ── Update DLQ entry ─────────────────────────────────────────────────────
    dlq_collection.update_one(
        {"job_id": job_id},
        {
            "$set": {
                "status":       "REQUEUED",
                "next_retry_at": datetime.utcnow() + timedelta(minutes=5),
            },
            "$inc": {"requeue_count": 1},
        },
    )

    # ── Reset job status in notification_jobs so worker can update it ────────
    jobs_collection.update_one(
        {"job_id": job_id},
        {"$set": {
            "status":     "QUEUED",
            "retry_meta": new_job["retry_meta"],
            "updated_at": datetime.utcnow(),
        }},
    )

    print(f"[DLQ] Requeued job {job_id} -> {queue_name} "
          f"(provider_index={current_index}, countdown={countdown}s)")


def mark_dropped(job, reason: str = ""):
    dlq_collection.update_one(
        {"job_id": job["job_id"]},
        {"$set": {
            "status":      "DROPPED",
            "drop_reason": reason,
            "dropped_at":  datetime.utcnow(),
        }},
    )
    jobs_collection.update_one(
        {"job_id": job["job_id"]},
        {"$set": {"status": "DROPPED", "updated_at": datetime.utcnow()}},
    )
    print(f"[DLQ] Dropped job {job['job_id']} — {reason}")

def run_worker():
    print("[DLQ] Processor started — polling every 5 seconds...")
    while True:
        try:
            process_dlq()
        except Exception as e:
            # Never let a single processing error kill the loop
            print(f"[DLQ] Processor error (continuing): {e}")
        time.sleep(5)


if __name__ == "__main__":
    run_worker()