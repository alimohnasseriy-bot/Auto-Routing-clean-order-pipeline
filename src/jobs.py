"""
src/jobs.py
===========
Phase 2 Scheduled Jobs using APScheduler.
"""

import time
import uuid
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import MongoClient

from config.settings import MONGODB_URI, MONGODB_DATABASE
from src.materialized_views import refresh_all_mvs

# Global scheduler instance
scheduler = BackgroundScheduler()

def get_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DATABASE]

def log_job_execution(job_name: str, status: str, start_time: datetime, end_time: datetime, details: str = ""):
    """Log the job execution to MongoDB 'jobs_log' collection."""
    db = get_db()
    db["jobs_log"].insert_one({
        "job_id": str(uuid.uuid4()),
        "job_name": job_name,
        "status": status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "details": details
    })

# ============================================================
# Job Definitions
# ============================================================

def job_refresh_mvs():
    """Job 1: Refresh Materialized Views."""
    start_time = datetime.now(timezone.utc)
    try:
        res = refresh_all_mvs()
        end_time = datetime.now(timezone.utc)
        log_job_execution("refresh_materialized_views", "SUCCESS", start_time, end_time, str(res))
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        log_job_execution("refresh_materialized_views", "FAILED", start_time, end_time, str(e))

def job_system_health_check():
    """Job 2: System Health Check (Logging DB stats)."""
    start_time = datetime.now(timezone.utc)
    try:
        db = get_db()
        stats = {
            "validated_count": db["orders_validated"].estimated_document_count(),
            "quarantine_count": db["orders_quarantine"].estimated_document_count()
        }
        end_time = datetime.now(timezone.utc)
        log_job_execution("system_health_check", "SUCCESS", start_time, end_time, str(stats))
    except Exception as e:
        end_time = datetime.now(timezone.utc)
        log_job_execution("system_health_check", "FAILED", start_time, end_time, str(e))

# ============================================================
# Scheduler Control
# ============================================================

def start_scheduler():
    """Initialize and start the scheduler."""
    if not scheduler.running:
        # Schedule the MV refresh every 1 hour
        scheduler.add_job(job_refresh_mvs, 'interval', hours=1, id='job_refresh_mvs', replace_existing=True)
        # Schedule the health check every 30 minutes
        scheduler.add_job(job_system_health_check, 'interval', minutes=30, id='job_system_health_check', replace_existing=True)
        scheduler.start()
        print("[OK] Scheduler started successfully.")

def shutdown_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        print("[OK] Scheduler stopped.")

def run_job_manually(job_id: str):
    """Run a specific job manually for testing/API purposes."""
    if job_id == "job_refresh_mvs":
        job_refresh_mvs()
        return {"status": "Job executed", "job_id": job_id}
    elif job_id == "job_system_health_check":
        job_system_health_check()
        return {"status": "Job executed", "job_id": job_id}
    else:
        raise ValueError(f"Unknown job_id: {job_id}")
