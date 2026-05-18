"""
Files Router (Production)
==========================
POST   /api/files/upload
GET    /api/files/
GET    /api/files/{id}
DELETE /api/files/{id}

Fixes:
  1. Atomic file limit check — race condition khatam (find_one_and_update)
  2. Proper CSV row count — pandas se, naive split nahi
  3. Subscription status check — halted ya cancelled user Pro features nahi paa sakta
  4. PLAN_LIMITS — auth.py se import, duplicate nahi
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from datetime import datetime, timezone
from bson import ObjectId
import base64
import io
import logging

from app.database import get_db
from app.routers.auth import get_current_user, PLAN_LIMITS
from app.services.cloudinary_service import upload_csv_to_cloudinary, delete_from_cloudinary

logger = logging.getLogger(__name__)
router = APIRouter()

# Pro plan ke liye valid subscription states
ACTIVE_SUBSCRIPTION_STATES = {"authenticated", "active"}


def _is_pro_active(user: dict) -> bool:
    """
    User ka plan 'pro' hai AND subscription active/authenticated hai.
    halted ya cancelled hone pe Pro features block hote hain.
    """
    if user.get("plan") != "pro":
        return False
    sub_status = user.get("subscription_status")
    # subscription_status None ho sakta hai purane users ke liye — unhe Pro maano
    if sub_status is None:
        return True
    return sub_status in ACTIVE_SUBSCRIPTION_STATES


# ── UPLOAD ───────────────────────────────────────────────
@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db=Depends(get_db),
    user=Depends(get_current_user),
):
    if not file.filename.endswith((".csv", ".txt")):
        raise HTTPException(status_code=400, detail="Sirf CSV/TXT files allowed hain")

    # Plan determine karo (subscription status check ke saath)
    effective_plan = "pro" if _is_pro_active(user) else "free"
    limits         = PLAN_LIMITS[effective_plan]

    # ── Atomic file limit check — race condition fix ──────
    # Pehle check karo, phir increment — non-atomic tha (bug fix)
    if limits["files_per_month"] != -1:
        result = await db.users.find_one_and_update(
            {
                "firebase_uid":    user["firebase_uid"],
                "files_used_month": {"$lt": limits["files_per_month"]},
            },
            {"$inc": {"files_used_month": 1}},
            return_document=True,  # updated doc return karo
        )
        if result is None:
            raise HTTPException(
                status_code=403,
                detail=f"Monthly limit ({limits['files_per_month']} files) reach ho gayi! Pro upgrade karo."
            )
    # Pro unlimited — sirf increment karo
    else:
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$inc": {"files_used_month": 1}}
        )

    # ── File read ─────────────────────────────────────────
    content = await file.read()
    if len(content) > 50 * 1024 * 1024:
        # Limit se badi file upload ki — increment rollback karo
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$inc": {"files_used_month": -1}}
        )
        raise HTTPException(status_code=413, detail="File too large — max 50MB allowed hai")

    # ── Row count — pandas se (naive split fix) ───────────
    try:
        import pandas as pd
        # nrows limit + 1 taaki pata chale limit exceed hua ya nahi
        max_rows = limits["max_rows"]
        read_limit = (max_rows + 2) if max_rows != -1 else None

        df = pd.read_csv(
            io.BytesIO(content),
            nrows=read_limit,
            low_memory=False,
            on_bad_lines="skip",
        )
        row_count = len(df)
        col_count = len(df.columns)
    except Exception as e:
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$inc": {"files_used_month": -1}}
        )
        raise HTTPException(status_code=400, detail=f"CSV parse error: {str(e)}")

    # ── Row limit check ───────────────────────────────────
    if limits["max_rows"] != -1 and row_count > limits["max_rows"]:
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$inc": {"files_used_month": -1}}
        )
        raise HTTPException(
            status_code=403,
            detail=f"Free plan mein max {limits['max_rows']} rows. File mein {row_count} rows hain. Pro upgrade karo!"
        )

    # ── Cloudinary upload (Pro + configured) ─────────────
    cloud_url = None
    if _is_pro_active(user) and user.get("cloudinary_cloud_name") and user.get("cloudinary_preset"):
        try:
            cloud_url = await upload_csv_to_cloudinary(
                content=content,
                filename=file.filename,
                cloud_name=user["cloudinary_cloud_name"],
                upload_preset=user["cloudinary_preset"],
                folder=f"dataclean/{user['firebase_uid']}/originals",
            )
        except Exception as e:
            logger.warning("Cloudinary upload failed (non-fatal): %s", str(e))

    # ── Save to MongoDB ───────────────────────────────────
    now = datetime.now(timezone.utc)
    doc = {
        "user_firebase_uid": user["firebase_uid"],
        "original_name":     file.filename,
        "original_rows":     row_count,
        "original_cols":     col_count,
        "clean_rows":        0,
        "clean_cols":        0,
        "file_size_kb":      round(len(content) / 1024, 2),
        "cloudinary_url":    cloud_url,
        "clean_csv_url":     None,
        "status":            "pending",
        "steps_applied":     [],
        "cleaning_report":   {},
        "has_analysis":      False,
        "analysis_id":       None,
        "created_at":        now,
    }
    result = await db.file_records.insert_one(doc)

    return {
        "file_id":        str(result.inserted_id),
        "name":           file.filename,
        "rows":           row_count,
        "cols":           col_count,
        "size_kb":        doc["file_size_kb"],
        "cloudinary_url": cloud_url,
        "status":         "pending",
        "content_b64":    base64.b64encode(content).decode(),
    }


# ── LIST ──────────────────────────────────────────────────
@router.get("/")
async def list_files(
    skip: int = 0, limit: int = 30,
    db=Depends(get_db), user=Depends(get_current_user)
):
    if not _is_pro_active(user):
        raise HTTPException(status_code=403, detail="File history Pro plan mein available hai")

    cursor = db.file_records.find(
        {"user_firebase_uid": user["firebase_uid"]}
    ).sort("created_at", -1).skip(skip).limit(min(limit, 100))

    return [_file_dict(f) async for f in cursor]


# ── SINGLE ────────────────────────────────────────────────
@router.get("/{file_id}")
async def get_file(file_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    try:
        f = await db.file_records.find_one(
            {"_id": ObjectId(file_id), "user_firebase_uid": user["firebase_uid"]}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")
    if not f:
        raise HTTPException(status_code=404, detail="File not found")
    return _file_dict(f)


# ── DELETE ────────────────────────────────────────────────
@router.delete("/{file_id}")
async def delete_file(file_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    try:
        oid = ObjectId(file_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")

    f = await db.file_records.find_one(
        {"_id": oid, "user_firebase_uid": user["firebase_uid"]}
    )
    if not f:
        raise HTTPException(status_code=404, detail="File not found")

    if f.get("cloudinary_url") and user.get("cloudinary_cloud_name"):
        try:
            await delete_from_cloudinary(f["cloudinary_url"], user["cloudinary_cloud_name"])
        except Exception as e:
            logger.warning("Cloudinary delete failed (non-fatal): %s", str(e))

    await db.file_records.delete_one({"_id": oid})
    await db.cleaning_jobs.delete_many({"file_id": file_id})
    await db.analyses.delete_many({"file_id": file_id})
    return {"message": "File delete ho gayi"}


# ── Serializer ────────────────────────────────────────────
def _file_dict(f: dict) -> dict:
    return {
        "id":            str(f["_id"]),
        "name":          f.get("original_name"),
        "original_rows": f.get("original_rows", 0),
        "original_cols": f.get("original_cols", 0),
        "clean_rows":    f.get("clean_rows", 0),
        "clean_cols":    f.get("clean_cols", 0),
        "size_kb":       f.get("file_size_kb", 0),
        "cloudinary_url":  f.get("cloudinary_url"),
        "clean_csv_url":   f.get("clean_csv_url"),
        "status":          f.get("status"),
        "has_analysis":    f.get("has_analysis", False),
        "report":          f.get("cleaning_report", {}),
        "created_at":      f["created_at"].isoformat() if f.get("created_at") else None,
    }
