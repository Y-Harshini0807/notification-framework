from celery import Celery
from kombu import Queue, Exchange

celery_app = Celery(
    "tasks",
    broker="amqp://guest:guest@localhost:5672//",
    # Use JSON serialiser so job dicts (with ISO datetime strings) travel safely
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

app = Celery('tasks')
app.conf.task_queues = (
    Queue('important', 
          exchange=Exchange('important'),
          queue_arguments={'x-max-length': 1500} # Limits to 100 messages
    ),
)

# One queue per channel — matches queue_name field in notification_jobs
celery_app.conf.task_queues = (
    Queue("email-notify-q"),
    Queue("sms-notify-q"),
    Queue("whatsapp-notify-q"),
    Queue("push-notify-q"),
)
celery_app.conf.task_create_missing_queues = True

# Route the send_notification task — queue is chosen dynamically at dispatch time
# via apply_async(queue=...) in main.py, so no static routing needed here.
celery_app.conf.task_routes = {}

# Retry policy defaults (overridden per-job by retry_policy in job_doc)
celery_app.conf.task_acks_late           = True   # ack only after task completes
celery_app.conf.worker_prefetch_multiplier = 1     # fair dispatch across workers
