"""
Firebase Auth Router
====================
Firebase token verify karo → MongoDB se user data lo/banao
- POST /api/auth/sync        — Firebase login ke baad user sync karo
- GET  /api/auth/me          — Current user info
- PUT  /api/auth/me          — Name update
- POST /api/auth/cloudinary-config
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

import firebase_admin
from firebase_admin import auth as firebase_auth

from app.database import get_db

router = APIRouter()
security = HTTPBearer()


# ── Schemas ──────────────────────────────────────────────
class SyncRequest(BaseModel):
    name: Optional[str] = None   # signup pe naam bhejte hain

class UserUpdate(BaseModel):
    name: Optional[str] = None

class CloudinaryConfig(BaseModel):
    cloud_name: str
    upload_preset: str


# ── Firebase Token Verify ─────────────────────────────────
async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(security),
    db=Depends(get_db)
):
    token = creds.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid Firebase token: {str(e)}")

    uid   = decoded["uid"]
    email = decoded.get("email", "")

    # MongoDB se user lo
    user = await db.users.find_one({"firebase_uid": uid})
    if not user:
        raise HTTPException(status_code=404, detail="User not found. /sync call karo pehle.")
    return user


# ── SYNC — Firebase login ke baad call karo ──────────────
@router.post("/sync")
async def sync_user(req: SyncRequest, creds: HTTPAuthorizationCredentials = Depends(security), db=Depends(get_db)):
    """
    Frontend pe Firebase login hone ke baad yeh call karo.
    MongoDB mein user create ya update karta hai.
    """
    token = creds.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid Firebase token: {str(e)}")

    uid        = decoded["uid"]
    email      = decoded.get("email", "")
    firebase_name = decoded.get("name", "") or req.name or email.split("@")[0]

    now = datetime.utcnow()

    # Upsert — already hai toh update, nahi hai toh create
    existing = await db.users.find_one({"firebase_uid": uid})

    if not existing:
        doc = {
            "firebase_uid":         uid,
            "email":                email,
            "name":                 firebase_name,
            "plan":                 "free",
            "files_used_month":     0,
            "month_reset":          now.strftime("%Y-%m"),
            "is_active":            True,
            "cloudinary_cloud_name": None,
            "cloudinary_preset":    None,
            "razorpay_payment_id":  None,
            "created_at":           now,
            "updated_at":           now,
        }
        result = await db.users.insert_one(doc)
        doc["_id"] = result.inserted_id
        user = doc
    else:
        # Month reset check
        current_month = now.strftime("%Y-%m")
        update_fields = {"updated_at": now}
        if existing.get("month_reset") != current_month:
            update_fields["files_used_month"] = 0
            update_fields["month_reset"]      = current_month
        await db.users.update_one({"firebase_uid": uid}, {"$set": update_fields})
        user = await db.users.find_one({"firebase_uid": uid})

    return user_to_dict(user)


# ── ME ───────────────────────────────────────────────────
@router.get("/me")
async def get_me(user=Depends(get_current_user), db=Depends(get_db)):
    # Month reset check
    current_month = datetime.utcnow().strftime("%Y-%m")
    if user.get("month_reset") != current_month:
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$set": {"files_used_month": 0, "month_reset": current_month}}
        )
        user["files_used_month"] = 0
    return user_to_dict(user)


# ── UPDATE NAME ──────────────────────────────────────────
@router.put("/me")
async def update_me(req: UserUpdate, user=Depends(get_current_user), db=Depends(get_db)):
    update = {"updated_at": datetime.utcnow()}
    if req.name:
        update["name"] = req.name.strip()
    await db.users.update_one({"firebase_uid": user["firebase_uid"]}, {"$set": update})
    updated = await db.users.find_one({"firebase_uid": user["firebase_uid"]})
    return user_to_dict(updated)


# ── CLOUDINARY CONFIG ────────────────────────────────────
@router.post("/cloudinary-config")
async def save_cloudinary_config(
    config: CloudinaryConfig,
    user=Depends(get_current_user),
    db=Depends(get_db)
):
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {"$set": {
            "cloudinary_cloud_name": config.cloud_name.strip(),
            "cloudinary_preset":     config.upload_preset.strip(),
            "updated_at":            datetime.utcnow()
        }}
    )
    return {"message": "Cloudinary config save ho gaya!", "cloud_name": config.cloud_name}


# ── Helper ───────────────────────────────────────────────
def user_to_dict(user: dict) -> dict:
    return {
        "id":               str(user.get("_id", "")),
        "firebase_uid":     user.get("firebase_uid", ""),
        "email":            user.get("email", ""),
        "name":             user.get("name", ""),
        "plan":             user.get("plan", "free"),
        "files_used_month": user.get("files_used_month", 0),
        "has_cloudinary":   bool(user.get("cloudinary_cloud_name")),
        "created_at":       user["created_at"].isoformat() if user.get("created_at") else None,
    }
