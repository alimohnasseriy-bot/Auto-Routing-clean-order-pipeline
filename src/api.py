"""
src/api.py
==========
Phase 2 FastAPI application serving as a unified execution & testing interface.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from typing import Dict, Any

from src.db_ops import (
    create_indexes, explain_query,
    query_customer_orders, query_quarantined_by_reason, query_orders_by_city_status,
    query_top_valuable_orders, query_orders_in_date_range,
    aggregate_sales_by_city, aggregate_top_products, aggregate_top_customers,
    aggregate_sales_by_date, aggregate_orders_by_status
)
from src.materialized_views import refresh_all_mvs
from src.jobs import start_scheduler, shutdown_scheduler, run_job_manually

# ============================================================
# Lifespan Events (Startup & Shutdown)
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_scheduler()
    yield
    # Shutdown
    shutdown_scheduler()

app = FastAPI(
    title="Midterm Data Pipeline API",
    description="Unified interface to test Phase 2 features (Indexes, Queries, MVs, Jobs).",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================
# Routes
# ============================================================

@app.get("/health")
def get_health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "data-pipeline-api"}

@app.post("/ingest")
def post_ingest(file_path: str = "data/orders_small_sample.csv"):
    """
    Triggers the main pipeline ingestion.
    Re-uses the Phase 1 logic without building a new pipeline.
    """
    from src.elt_pipeline import run_pipeline
    try:
        metrics = run_pipeline(file_path=file_path)
        return {"status": "success", "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/indexes")
def post_indexes():
    """Creates Phase 2 indexes on the database."""
    try:
        idx_names = create_indexes()
        return {"status": "success", "created_indexes": idx_names}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queries")
def get_queries_list():
    """Lists available queries."""
    return {
        "available_queries": [
            "customer_orders",
            "quarantined_by_reason",
            "orders_by_city_status",
            "top_valuable_orders",
            "orders_in_date_range"
        ]
    }

@app.get("/queries/{name}")
def get_query_by_name(name: str, customer_id: str = None, reason: str = None, city: str = None, status: str = None, start_date: str = None, end_date: str = None, limit: int = 10, explain: bool = False):
    """Executes a specific query by name. Pass `explain=true` to get execution stats instead of data."""
    query_filter = {}
    
    if name == "customer_orders":
        if not customer_id: raise HTTPException(status_code=400, detail="Missing customer_id")
        query_filter = {"customer_id": customer_id}
        func = lambda: query_customer_orders(customer_id)
        
    elif name == "quarantined_by_reason":
        if not reason: raise HTTPException(status_code=400, detail="Missing reason")
        query_filter = {"quarantine_reasons": reason}
        func = lambda: query_quarantined_by_reason(reason)
        
    elif name == "orders_by_city_status":
        if not city or not status: raise HTTPException(status_code=400, detail="Missing city or status")
        query_filter = {"city": city, "status": status}
        func = lambda: query_orders_by_city_status(city, status)
        
    elif name == "top_valuable_orders":
        query_filter = {} # Just sort/limit
        func = lambda: query_top_valuable_orders(limit)
        
    elif name == "orders_in_date_range":
        if not start_date or not end_date: raise HTTPException(status_code=400, detail="Missing dates")
        query_filter = {"order_date": {"$gte": start_date, "$lte": end_date}}
        func = lambda: query_orders_in_date_range(start_date, end_date)
        
    else:
        raise HTTPException(status_code=404, detail="Query not found")

    if explain:
        # Assuming the collection depends on the query name, most are validated_col
        col_name = "orders_quarantine" if name == "quarantined_by_reason" else "orders_validated"
        return explain_query(query_filter, collection_name=col_name)
    else:
        return {"data": func()}

@app.get("/aggregations")
def get_aggregations_list():
    """Lists available aggregations."""
    return {
        "available_aggregations": [
            "sales_by_city",
            "top_products",
            "top_customers",
            "sales_by_date",
            "orders_by_status"
        ]
    }

@app.get("/aggregations/{name}")
def get_aggregation_by_name(name: str):
    """Executes a specific aggregation by name."""
    aggs = {
        "sales_by_city": aggregate_sales_by_city,
        "top_products": aggregate_top_products,
        "top_customers": aggregate_top_customers,
        "sales_by_date": aggregate_sales_by_date,
        "orders_by_status": aggregate_orders_by_status
    }
    if name not in aggs:
        raise HTTPException(status_code=404, detail="Aggregation not found")
    return {"data": aggs[name]()}

@app.post("/refresh-mv")
def post_refresh_mv():
    """Triggers an incremental refresh of materialized views."""
    try:
        res = refresh_all_mvs()
        return {"status": "success", "details": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs")
def get_jobs_list():
    """List available scheduled jobs."""
    return {
        "available_jobs": [
            "job_refresh_mvs",
            "job_system_health_check"
        ]
    }

@app.post("/jobs/{name}/run")
def post_run_job(name: str):
    """Manually run a background job by name."""
    try:
        res = run_job_manually(name)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Running instructions: uvicorn src.api:app --reload
