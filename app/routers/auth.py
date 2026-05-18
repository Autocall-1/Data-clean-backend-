"""
Firebase Auth Router (Production)
===================================
- POST /api/auth/sync        — Firebase login ke baad user sync karo
- GET  /api/auth/me          — Current user + plan limits
- PUT  /api/auth/me          — Name update
- POST /api/auth/cloudinary-config

Fixes:
  1. get_current_user — 404 nahi dega, auto-upsert karega
  2. month reset logic — ek jagah centralized (check_and_reset_month)
  3. user_to_dict — plan limits aur subscription_status bhi return karta hai
  4. datetime.utcnow() → datetime.now(timezone.utc) (deprecation fix)
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import logging

import firebase_admin
from firebase_admin import auth as firebase_auth

from app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter()
security = HTTPBearer()

# ── Plan limits — ek jagah define, sabhi routers import karein ──
PLAN_LIMITS = {
    "free": {
        "files_per_month": 5,
        "max_rows":        1_000,
        "cloud":           False,
        "pdf":             False,
        "ai_analysis":     False,
    },
    "pro": {
        "files_per_month": -1,      # unlimited
        "max_rows":        -1,
        "cloud":           True,
        "pdf":             True,
        "ai_analysis":     True,
    },
}


# ── Schemas ───────────────────────────────────────────────
class SyncRequest(BaseModel):
    name: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None

class CloudinaryConfig(BaseModel):
    cloud_name:    str
    upload_preset: str


# ── Helper: month reset (centralized) ────────────────────
async def check_and_reset_month(user: dict, db) -> dict:
    """
    Agar naya mahina hai toh files_used_month reset karo.
    Updated user dict return karta hai.
    """
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    if user.get("month_reset") != current_month:
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$set": {"files_used_month": 0, "month_reset": current_month,
                      "updated_at": datetime.now(timezone.utc)}}
        )
        user["files_used_month"] = 0
        user["month_reset"] = current_month
    return user


# ── Firebase Token Verify + Auto-upsert ──────────────────
async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(security),
    db=Depends(get_db)
):
    """
    Firebase token verify karo → MongoDB user fetch ya auto-create.
    Fix: pehle 404 deta tha agar /sync nahi kiya — ab auto-upsert karta hai.
    """
    token = creds.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid Firebase token: {str(e)}")

    uid   = decoded["uid"]
    email = decoded.get("email", "")
    now   = datetime.now(timezone.utc)

    user = await db.users.find_one({"firebase_uid": uid})

    if not user:
        # Auto-create — /sync bhulne pe bhi kaam karega
        doc = _default_user_doc(uid, email, decoded.get("name", "") or email.split("@")[0], now)
        result = await db.users.insert_one(doc)
        doc["_id"] = result.inserted_id
        user = doc
        logger.info("Auto-created user: uid=%s", uid)

    # Month reset check
    user = await check_and_reset_month(user, db)
    return user


# ── Default user document ─────────────────────────────────
def _default_user_doc(uid: str, email: str, name: str, now: datetime) -> dict:
    return {
        "firebase_uid":             uid,
        "email":                    email,
        "name":                     name,
        "plan":                     "free",
        "subscription_status":      None,       # created|authenticated|active|halted|cancelled
        "razorpay_subscription_id": None,
        "razorpay_last_payment_id": None,
        "pro_since":                None,
        "last_renewal_at":          None,
        "last_payment_error":       None,
        "files_used_month":         0,
        "month_reset":              now.strftime("%Y-%m"),
        "is_active":                True,
        "cloudinary_cloud_name":    None,
        "cloudinary_preset":        None,
        "created_at":               now,
        "updated_at":               now,
    }


# ── SYNC ─────────────────────────────────────────────────
@router.post("/sync")
async def sync_user(
    req: SyncRequest,
    creds: HTTPAuthorizationCredentials = Depends(security),
    db=Depends(get_db)
):
    """
    Frontend pe Firebase login ke baad call karo.
    MongoDB mein user create ya update karta hai.
    """
    token = creds.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid Firebase token: {str(e)}")

    uid           = decoded["uid"]
    email         = decoded.get("email", "")
    firebase_name = decoded.get("name", "") or req.name or email.split("@")[0]
    now           = datetime.now(timezone.utc)

    existing = await db.users.find_one({"firebase_uid": uid})

    if not existing:
        doc = _default_user_doc(uid, email, firebase_name, now)
        result = await db.users.insert_one(doc)
        doc["_id"] = result.inserted_id
        user = doc
    else:
        # Month reset + timestamp update
        update_fields = {"updated_at": now}
        current_month = now.strftime("%Y-%m")
        if existing.get("month_reset") != current_month:
            update_fields["files_used_month"] = 0
            update_fields["month_reset"]      = current_month
        await db.users.update_one({"firebase_uid": uid}, {"$set": update_fields})
        user = await db.users.find_one({"firebase_uid": uid})

    return user_to_dict(user)


# ── ME ────────────────────────────────────────────────────
@router.get("/me")
async def get_me(user=Depends(get_current_user)):
    """
    Current user info + plan limits.
    Fix: subscription_status aur limits bhi return karta hai.
    """
    return user_to_dict(user)


# ── UPDATE NAME ───────────────────────────────────────────
@router.put("/me")
async def update_me(
    req: UserUpdate,
    user=Depends(get_current_user),
    db=Depends(get_db)
):
    update = {"updated_at": datetime.now(timezone.utc)}
    if req.name and req.name.strip():
        update["name"] = req.name.strip()
    await db.users.update_one({"firebase_uid": user["firebase_uid"]}, {"$set": update})
    updated = await db.users.find_one({"firebase_uid": user["firebase_uid"]})
    return user_to_dict(updated)


# ── CLOUDINARY CONFIG ─────────────────────────────────────
@router.post("/cloudinary-config")
async def save_cloudinary_config(
    config: CloudinaryConfig,
    user=Depends(get_current_user),
    db=Depends(get_db)
):
    if user.get("plan") != "pro":
        raise HTTPException(status_code=403, detail="Cloudinary config Pro plan mein available hai")

    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {"$set": {
            "cloudinary_cloud_name": config.cloud_name.strip(),
            "cloudinary_preset":     config.upload_preset.strip(),
            "updated_at":            datetime.now(timezone.utc),
        }}
    )
    return {"message": "Cloudinary config save ho gaya!", "cloud_name": config.cloud_name}


# ── Serializer ────────────────────────────────────────────
def user_to_dict(user: dict) -> dict:
    """
    Fix: plan limits aur subscription_status bhi include kiya.
    Frontend ko alag /plans/status call nahi karni padegi.
    """
    plan   = user.get("plan", "free")
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])

    return {
        "id":               str(user.get("_id", "")),
        "firebase_uid":     user.get("firebase_uid", ""),
        "email":            user.get("email", ""),
        "name":             user.get("name", ""),
        "plan":             plan,
        "subscription_status":      user.get("subscription_status"),
        "razorpay_subscription_id": user.get("razorpay_subscription_id"),
        "files_used_month": user.get("files_used_month", 0),
        "has_cloudinary":   bool(user.get("cloudinary_cloud_name")),
        "limits":           limits,
        "pro_since":        user["pro_since"].isoformat() if user.get("pro_since") else None,
        "last_renewal_at":  user["last_renewal_at"].isoformat() if user.get("last_renewal_at") else None,
        "created_at":       user["created_at"].isoformat() if user.get("created_at") else None,
    }
