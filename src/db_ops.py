"""
src/db_ops.py
=============
Phase 2 Database Operations: Indexes, Queries, and Aggregations.
"""

from typing import List, Dict, Any
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from config.settings import MONGODB_URI, MONGODB_DATABASE, VALIDATED_COLLECTION, QUARANTINE_COLLECTION

def get_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DATABASE]

# ============================================================
# 1. Indexes & Explain
# ============================================================

def create_indexes() -> List[str]:
    """Create the required indexes (3 at least, including 1 compound)."""
    db = get_db()
    validated_col = db[VALIDATED_COLLECTION]
    
    indexes = [
        # Compound Index
        IndexModel([("customer_id", ASCENDING), ("order_date", DESCENDING)], name="idx_customer_date"),
        # Single Indexes
        IndexModel([("status", ASCENDING)], name="idx_status"),
        IndexModel([("city", ASCENDING)], name="idx_city")
    ]
    
    return validated_col.create_indexes(indexes)

def explain_query(query_dict: Dict, collection_name: str = VALIDATED_COLLECTION) -> Dict:
    """Run explain('executionStats') on a given query dictionary."""
    db = get_db()
    return db[collection_name].find(query_dict).explain()["executionStats"]

# ============================================================
# 2. Queries (5 Queries)
# ============================================================

def query_customer_orders(customer_id: str) -> List[Dict]:
    """1. Find orders for a specific customer."""
    db = get_db()
    return list(db[VALIDATED_COLLECTION].find({"customer_id": customer_id}, {"_id": 0}).limit(100))

def query_quarantined_by_reason(reason: str) -> List[Dict]:
    """2. Find quarantined records by specific reason."""
    db = get_db()
    return list(db[QUARANTINE_COLLECTION].find({"quarantine_reasons": reason}, {"_id": 0}).limit(100))

def query_orders_by_city_status(city: str, status: str) -> List[Dict]:
    """3. Find orders in a specific city with a specific status."""
    db = get_db()
    return list(db[VALIDATED_COLLECTION].find({"city": city, "status": status}, {"_id": 0}).limit(100))

def query_top_valuable_orders(limit: int = 10) -> List[Dict]:
    """4. Get top N most valuable orders by total_amount."""
    db = get_db()
    # Assuming total_amount is stored as numeric after quality rules, otherwise we sort differently or parse
    # Here we sort descending.
    return list(db[VALIDATED_COLLECTION].find({}, {"_id": 0}).sort("total_amount", DESCENDING).limit(limit))

def query_orders_in_date_range(start_date: str, end_date: str) -> List[Dict]:
    """5. Find orders within a specific date range."""
    db = get_db()
    return list(db[VALIDATED_COLLECTION].find({
        "order_date": {"$gte": start_date, "$lte": end_date}
    }, {"_id": 0}).limit(100))


# ============================================================
# 3. Aggregations (5 Reports)
# ============================================================

def aggregate_sales_by_city() -> List[Dict]:
    """1. Total sales and order count grouped by city."""
    db = get_db()
    pipeline = [
        {"$group": {
            "_id": "$city",
            "total_sales": {"$sum": {"$toDouble": "$total_amount"}},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"total_sales": DESCENDING}}
    ]
    return list(db[VALIDATED_COLLECTION].aggregate(pipeline))

def aggregate_top_products(limit: int = 10) -> List[Dict]:
    """2. Top products by quantity sold."""
    db = get_db()
    pipeline = [
        {"$unwind": "$items_json"},
        {"$group": {
            "_id": "$items_json.product_name",
            "total_quantity": {"$sum": {"$toDouble": "$items_json.quantity"}},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"total_quantity": DESCENDING}},
        {"$limit": limit}
    ]
    return list(db[VALIDATED_COLLECTION].aggregate(pipeline))

def aggregate_top_customers(limit: int = 10) -> List[Dict]:
    """3. Top customers by total spent."""
    db = get_db()
    pipeline = [
        {"$group": {
            "_id": "$customer_id",
            "customer_name": {"$first": "$customer_name"},
            "total_spent": {"$sum": {"$toDouble": "$total_amount"}},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"total_spent": DESCENDING}},
        {"$limit": limit}
    ]
    return list(db[VALIDATED_COLLECTION].aggregate(pipeline))

def aggregate_sales_by_date() -> List[Dict]:
    """4. Daily sales summary."""
    db = get_db()
    pipeline = [
        {"$group": {
            "_id": {"$substr": ["$order_date", 0, 10]}, # group by YYYY-MM-DD
            "daily_revenue": {"$sum": {"$toDouble": "$total_amount"}},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"_id": ASCENDING}}
    ]
    return list(db[VALIDATED_COLLECTION].aggregate(pipeline))

def aggregate_orders_by_status() -> List[Dict]:
    """5. Order distribution by status."""
    db = get_db()
    pipeline = [
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": DESCENDING}}
    ]
    return list(db[VALIDATED_COLLECTION].aggregate(pipeline))

