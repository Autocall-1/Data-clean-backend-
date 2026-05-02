"""Cloudinary Router"""
from fastapi import APIRouter, Depends, HTTPException
from app.database import get_db
from app.routers.auth import get_current_user
from app.services.cloudinary_service import upload_csv_to_cloudinary

router = APIRouter()

@router.get("/status")
def cloudinary_status(user=Depends(get_current_user)):
    configured = bool(user.get("cloudinary_cloud_name") and user.get("cloudinary_preset"))
    return {"configured": configured, "cloud_name": user.get("cloudinary_cloud_name") if configured else None}

@router.post("/test")
async def test_cloudinary(user=Depends(get_current_user)):
    if not user.get("cloudinary_cloud_name"):
        raise HTTPException(status_code=400, detail="Cloudinary config nahi hai")
    test_content = b"id,name,value\n1,test,100\n"
    url = await upload_csv_to_cloudinary(
        content=test_content, filename="test.csv",
        cloud_name=user["cloudinary_cloud_name"],
        upload_preset=user["cloudinary_preset"],
        folder=f"dataclean/{user['firebase_uid']}/tests"
    )
    if not url:
        raise HTTPException(status_code=502, detail="Cloudinary test failed")
    return {"success": True, "message": "Connected!", "test_url": url}
