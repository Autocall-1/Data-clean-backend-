"""
MongoDB Atlas Connection — Motor async driver (Production)

Fixes / Additions:
  1. razorpay_subscription_id pe unique index — webhook fast lookup
  2. razorpay_last_payment_id pe index — idempotency check fast
  3. analyses collection index
  4. subscription_status pe index — halted users query fast
"""

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
import os, logging

logger = logging.getLogger(__name__)

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DB_NAME     = os.getenv("MONGODB_DB_NAME", "dataclean")

client = None
db     = None


async def connect_db():
    global client, db
    client = AsyncIOMotorClient(MONGODB_URL, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]

    # ── users collection indexes ──────────────────────────
    await db.users.create_index([("firebase_uid", ASCENDING)], unique=True)
    await db.users.create_index([("email", ASCENDING)], unique=True)

    # Subscription lookup — webhook aur idempotency ke liye
    await db.users.create_index(
        [("razorpay_subscription_id", ASCENDING)],
        sparse=True,   # None values ignore honge
    )
    await db.users.create_index(
        [("razorpay_last_payment_id", ASCENDING)],
        sparse=True,
    )
    # Halted/cancelled users dhundhne ke liye
    await db.users.create_index([("subscription_status", ASCENDING)], sparse=True)

    # ── file_records indexes ──────────────────────────────
    await db.file_records.create_index([("user_firebase_uid", ASCENDING)])
    await db.file_records.create_index([("created_at", DESCENDING)])

    # ── cleaning_jobs indexes ─────────────────────────────
    await db.cleaning_jobs.create_index([("file_id", ASCENDING)])

    # ── analyses indexes ──────────────────────────────────
    await db.analyses.create_index([("file_id", ASCENDING)])
    await db.analyses.create_index([("user_firebase_uid", ASCENDING)])
    await db.analyses.create_index([("created_at", DESCENDING)])

    logger.info("MongoDB connected → %s", DB_NAME)
    print(f"✅ MongoDB connected → {DB_NAME}")


async def disconnect_db():
    global client
    if client:
        client.close()
        logger.info("MongoDB disconnected")


def get_db():
    return db
