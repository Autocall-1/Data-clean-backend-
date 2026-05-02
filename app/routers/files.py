"""
Files Router — Firebase Auth + MongoDB + Cloudinary
POST   /api/files/upload
GET    /api/files/
GET    /api/files/{id}
DELETE /api/files/{id}
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from datetime import datetime
from bson import ObjectId
import base64

from app.database import get_db
from app.routers.auth import get_current_user
from app.services.cloudinary_service import upload_csv_to_cloudinary, delete_from_cloudinary

router = APIRouter()

PLAN_LIMITS = {
    "free": {"files_per_month": 5,      "max_rows": 1000},
    "pro":  {"files_per_month": 999999, "max_rows": 999999},
}


# ── UPLOAD ───────────────────────────────────────────────
@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db=Depends(get_db),
    user=Depends(get_current_user),
):
    if not file.filename.endswith((".csv", ".txt")):
        raise HTTPException(status_code=400, detail="Sirf CSV files allowed hain")

    plan   = user.get("plan", "free")
    limits = PLAN_LIMITS[plan]

    if user.get("files_used_month", 0) >= limits["files_per_month"]:
        raise HTTPException(status_code=403,
            detail=f"Monthly limit ({limits['files_per_month']} files) reach ho gayi! Pro upgrade karo.")

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large — max 50MB")

    # Row count
    try:
        text  = content.decode("utf-8", errors="replace")
        lines = [l for l in text.split("\n") if l.strip()]
        row_count = max(0, len(lines) - 1)
        col_count = len(lines[0].split(",")) if lines else 0
    except Exception:
        raise HTTPException(status_code=400, detail="CSV parse error")

    if row_count > limits["max_rows"]:
        raise HTTPException(status_code=403,
            detail=f"Free plan mein max {limits['max_rows']} rows. File mein {row_count} rows hain. Pro upgrade karo!")

    # Cloudinary upload (Pro only ya configured)
    cloud_url = None
    if user.get("cloudinary_cloud_name") and user.get("cloudinary_preset"):
        cloud_url = await upload_csv_to_cloudinary(
            content=content,
            filename=file.filename,
            cloud_name=user["cloudinary_cloud_name"],
            upload_preset=user["cloudinary_preset"],
            folder=f"dataclean/{user['firebase_uid']}/originals"
        )

    # Save to MongoDB
    now = datetime.utcnow()
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
        "created_at":        now,
    }
    result = await db.file_records.insert_one(doc)

    # Increment usage
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {"$inc": {"files_used_month": 1}}
    )

    return {
        "file_id":       str(result.inserted_id),
        "name":          file.filename,
        "rows":          row_count,
        "cols":          col_count,
        "size_kb":       doc["file_size_kb"],
        "cloudinary_url": cloud_url,
        "status":        "pending",
        "content_b64":   base64.b64encode(content).decode(),
    }


# ── LIST (Pro only) ──────────────────────────────────────
@router.get("/")
async def list_files(
    skip: int = 0, limit: int = 30,
    db=Depends(get_db), user=Depends(get_current_user)
):
    if user.get("plan") != "pro":
        raise HTTPException(status_code=403, detail="File history Pro plan mein available hai")

    cursor = db.file_records.find(
        {"user_firebase_uid": user["firebase_uid"]}
    ).sort("created_at", -1).skip(skip).limit(limit)

    return [_file_dict(f) async for f in cursor]


# ── SINGLE ───────────────────────────────────────────────
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


# ── DELETE ───────────────────────────────────────────────
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
        await delete_from_cloudinary(f["cloudinary_url"], user["cloudinary_cloud_name"])

    await db.file_records.delete_one({"_id": oid})
    await db.cleaning_jobs.delete_many({"file_id": file_id})
    return {"message": "File delete ho gayi"}


def _file_dict(f):
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
        "report":          f.get("cleaning_report", {}),
        "created_at":      f["created_at"].isoformat() if f.get("created_at") else None,
    }
