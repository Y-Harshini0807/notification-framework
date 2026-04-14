from datetime import datetime
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["notification_db"]

def log_event(type, message, level="INFO", job_id=None, queue=None):
    log = {
        "type": type,
        "message": message,
        "level": level,
        "job_id": job_id,
        "queue": queue,
        "created_at": datetime.utcnow()
    }
    db.logs.insert_one(log)
