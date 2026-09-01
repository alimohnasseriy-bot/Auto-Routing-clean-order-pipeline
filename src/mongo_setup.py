from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


# ============================================================
# MongoDB Configuration
# ============================================================

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "midterm_data_pipeline"


# ============================================================
# Connect to MongoDB
# ============================================================

def connect_to_mongodb():
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000
        )

        # Test connection
        client.admin.command("ping")

        print("[OK] MongoDB connection successful")

        db = client[DATABASE_NAME]

        return client, db

    except ConnectionFailure as error:
        print("[ERR] Failed to connect to MongoDB")
        print(f"Error: {error}")
        raise


# ============================================================
# Create Collections
# ============================================================

def create_collections(db):
    required_collections = [
        "orders_raw",
        "orders_validated",
        "orders_quarantine"
    ]

    existing_collections = db.list_collection_names()

    for collection_name in required_collections:

        if collection_name not in existing_collections:
            db.create_collection(collection_name)
            print(f"[OK] Collection created: {collection_name}")

        else:
            print(f"[OK] Collection already exists: {collection_name}")


# ============================================================
# Create Indexes
# ============================================================

def create_indexes(db):

    # Unique index for validated orders
    # id_order is the stable business key
    try:
        db.orders_validated.create_index(
            [("id_order", 1)],
            unique=True,
            name="unique_id_order"
        )

        print("[OK] Unique index created on orders_validated.id_order")

    except OperationFailure as error:
        print("[ERR] Failed to create unique index")
        print(f"Error: {error}")
        raise


# ============================================================
# Main Setup Function
# ============================================================

def setup_database():

    print("=" * 60)
    print("MongoDB Database Setup")
    print("=" * 60)

    client = None

    try:
        # Connect
        client, db = connect_to_mongodb()

        print(f"[OK] Database: {DATABASE_NAME}")

        # Create collections
        create_collections(db)

        # Create indexes
        create_indexes(db)

        print("=" * 60)
        print("[OK] MongoDB setup completed successfully")
        print("=" * 60)

        print("\nCollections:")
        for collection in db.list_collection_names():
            print(f"  - {collection}")

        print("\nIndexes on orders_validated:")
        for index in db.orders_validated.list_indexes():
            print(f"  - {index['name']}")

    finally:

        if client:
            client.close()
            print("\n[OK] MongoDB connection closed")


# ============================================================
# Run
# ============================================================

if __name__ == "__main__":
    setup_database()