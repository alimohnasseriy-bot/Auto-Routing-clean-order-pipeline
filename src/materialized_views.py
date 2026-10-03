"""
src/materialized_views.py
=========================
Phase 2 Materialized Views updating incrementally using $merge.
"""

from pymongo import MongoClient
from config.settings import MONGODB_URI, MONGODB_DATABASE, VALIDATED_COLLECTION
from datetime import datetime, timedelta, timezone

def get_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DATABASE]

def refresh_daily_sales_mv(days_back: int = 2) -> dict:
    """
    Refresh the 'mv_daily_sales_summary' materialized view.
    Instead of rebuilding the entire history, we only recalculate the last 'days_back' days
    and $merge the results into the view.
    """
    db = get_db()
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    pipeline = [
        # 1. Match only recent orders (incremental approach) AND ensure order_date exists
        {"$match": {
            "order_date": {"$gte": cutoff_date, "$type": "string"}
        }},
        # 2. Group by date
        {"$group": {
            "_id": {"$substr": ["$order_date", 0, 10]}, # YYYY-MM-DD
            "daily_revenue": {"$sum": {"$toDouble": "$total_amount"}},
            "order_count": {"$sum": 1}
        }},
        # 3. Merge into materialized view collection
        {"$merge": {
            "into": "mv_daily_sales_summary",
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }}
    ]
    
    db[VALIDATED_COLLECTION].aggregate(pipeline)
    return {"status": "success", "view": "mv_daily_sales_summary", "cutoff_date": cutoff_date}

def refresh_top_products_mv() -> dict:
    """
    Refresh the 'mv_top_products_summary' materialized view.
    Since product totals are global, we rebuild it or use $merge to upsert.
    We'll do a full refresh into $merge so the collection remains available during the update.
    """
    db = get_db()
    
    pipeline = [
        {"$unwind": "$items_json"},
        {"$match": {
            "items_json.product_name": {"$type": "string"}
        }},
        {"$group": {
            "_id": "$items_json.product_name",
            "total_quantity": {"$sum": {"$toDouble": "$items_json.quantity"}},
            "order_count": {"$sum": 1}
        }},
        {"$merge": {
            "into": "mv_top_products_summary",
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }}
    ]
    
    db[VALIDATED_COLLECTION].aggregate(pipeline)
    return {"status": "success", "view": "mv_top_products_summary"}

def refresh_all_mvs():
    """Helper to refresh all MVs."""
    res1 = refresh_daily_sales_mv()
    res2 = refresh_top_products_mv()
    return [res1, res2]
