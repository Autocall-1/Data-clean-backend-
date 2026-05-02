"""
Cleaning Router — Firebase Auth
POST /api/clean/run
GET  /api/clean/job/{job_id}
GET  /api/clean/{file_id}/report/pdf
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
from datetime import datetime
from bson import ObjectId
import io, time

from app.database import get_db
from app.routers.auth import get_current_user
from app.services.cleaner import DataCleaner
from app.services.pdf_service import generate_pdf_report
from app.services.cloudinary_service import upload_csv_to_cloudinary

router = APIRouter()


class CleanRequest(BaseModel):
    file_id:     str
    csv_content: str
    steps:       List[str]


@router.post("/run")
async def run_cleaning(
    req: CleanRequest,
    db=Depends(get_db),
    user=Depends(get_current_user)
):
    try:
        foid = ObjectId(req.file_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")

    file_rec = await db.file_records.find_one(
        {"_id": foid, "user_firebase_uid": user["firebase_uid"]}
    )
    if not file_rec:
        raise HTTPException(status_code=404, detail="File not found")

    # Free plan — outliers nahi
    steps = req.steps
    if user.get("plan", "free") == "free":
        steps = [s for s in steps if s != "outliers"]

    # Create job
    now = datetime.utcnow()
    job_doc = {
        "file_id":     req.file_id,
        "status":      "running",
        "progress":    0,
        "error":       None,
        "duration_sec": 0.0,
        "started_at":  now,
        "finished_at": None,
    }
    job_result = await db.cleaning_jobs.insert_one(job_doc)

    await db.file_records.update_one({"_id": foid}, {"$set": {"status": "processing"}})

    try:
        start   = time.time()
        cleaner = DataCleaner(req.csv_content)
        result  = cleaner.run(steps)
        duration = round(time.time() - start, 2)

        # Upload cleaned CSV to Cloudinary (Pro only)
        clean_url = None
        if user.get("plan") == "pro" and user.get("cloudinary_cloud_name"):
            clean_url = await upload_csv_to_cloudinary(
                content=result["clean_csv"].encode("utf-8"),
                filename=f"cleaned_{file_rec['original_name']}",
                cloud_name=user["cloudinary_cloud_name"],
                upload_preset=user["cloudinary_preset"],
                folder=f"dataclean/{user['firebase_uid']}/cleaned"
            )

        # Update MongoDB
        await db.file_records.update_one({"_id": foid}, {"$set": {
            "clean_rows":      result["clean_rows"],
            "clean_cols":      result["clean_cols"],
            "cleaning_report": result["report"],
            "steps_applied":   steps,
            "clean_csv_url":   clean_url,
            "status":          "done",
        }})
        await db.cleaning_jobs.update_one(
            {"_id": job_result.inserted_id},
            {"$set": {"status": "done", "progress": 100,
                      "duration_sec": duration, "finished_at": datetime.utcnow()}}
        )

        return {
            "job_id":       str(job_result.inserted_id),
            "status":       "done",
            "clean_csv":    result["clean_csv"],
            "clean_rows":   result["clean_rows"],
            "clean_cols":   result["clean_cols"],
            "report":       result["report"],
            "duration_sec": duration,
            "clean_csv_url": clean_url,
        }

    except Exception as e:
        await db.cleaning_jobs.update_one(
            {"_id": job_result.inserted_id},
            {"$set": {"status": "error", "error": str(e)}}
        )
        await db.file_records.update_one({"_id": foid}, {"$set": {"status": "error"}})
        raise HTTPException(status_code=500, detail=f"Cleaning failed: {str(e)}")


@router.get("/job/{job_id}")
async def get_job(job_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    try:
        job = await db.cleaning_jobs.find_one({"_id": ObjectId(job_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": str(job["_id"]), "status": job.get("status"),
            "progress": job.get("progress", 0), "error": job.get("error")}


@router.get("/{file_id}/report/pdf")
async def download_pdf(file_id: str, db=Depends(get_db), user=Depends(get_current_user)):
    if user.get("plan") != "pro":
        raise HTTPException(status_code=403, detail="PDF reports Pro plan mein hain")
    try:
        f = await db.file_records.find_one(
            {"_id": ObjectId(file_id), "user_firebase_uid": user["firebase_uid"]}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")
    if not f:
        raise HTTPException(status_code=404, detail="File not found")
    if f.get("status") != "done":
        raise HTTPException(status_code=400, detail="File abhi clean nahi hui")

    pdf_bytes = generate_pdf_report(user=user, file_rec=f, report=f.get("cleaning_report", {}))
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="report_{f["original_name"]}.pdf"'}
    )
