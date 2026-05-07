"""
MongoDB Atlas Connection — Motor async driver
"""

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
import os

MONGODB_URL = 
DB_NAME     = os.getenv("MONGODB_DB_NAME", "dataclean")

client = None
db     = None


async def connect_db():
    global client, db
    client = AsyncIOMotorClient(MONGODB_URL, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]

    # Indexes
    await db.users.create_index([("firebase_uid", ASCENDING)], unique=True)
    await db.users.create_index([("email", ASCENDING)], unique=True)
    await db.file_records.create_index([("user_firebase_uid", ASCENDING)])
    await db.file_records.create_index([("created_at", DESCENDING)])
    await db.cleaning_jobs.create_index([("file_id", ASCENDING)])

    print(f"✅ MongoDB connected → {DB_NAME}")


async def disconnect_db():
    global client
    if client:
        client.close()
        print("👋 MongoDB disconnected")


def get_db():
    return db
