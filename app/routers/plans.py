"""
Plans Router — Razorpay + Firebase Auth
POST /api/plans/create-order
POST /api/plans/verify-payment
GET  /api/plans/status
POST /api/plans/cancel
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from datetime import datetime
import httpx, hmac, hashlib, base64, os

from app.database import get_db
from app.routers.auth import get_current_user

router = APIRouter()

RAZORPAY_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "rzp_test_XXXXXXXXXX")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "your_secret")
PLAN_AMOUNT_PAISE   = 49900  # ₹499


class VerifyPaymentRequest(BaseModel):
    razorpay_order_id:   str
    razorpay_payment_id: str
    razorpay_signature:  str


@router.post("/create-order")
async def create_order(user=Depends(get_current_user), db=Depends(get_db)):
    if user.get("plan") == "pro":
        raise HTTPException(status_code=400, detail="Already Pro plan pe ho!")
    creds = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode()).decode()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                "https://api.razorpay.com/v1/orders",
                headers={"Authorization": f"Basic {creds}"},
                json={
                    "amount": PLAN_AMOUNT_PAISE, "currency": "INR",
                    "receipt": f"order_{user['firebase_uid'][:8]}",
                    "notes": {"firebase_uid": user["firebase_uid"], "email": user["email"]}
                }
            )
        if res.status_code != 200:
            raise HTTPException(status_code=502, detail="Razorpay order failed")
        order = res.json()
        return {
            "order_id": order["id"], "amount": order["amount"],
            "currency": "INR", "razorpay_key": RAZORPAY_KEY_ID,
            "prefill": {"name": user["name"], "email": user["email"]}
        }
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/verify-payment")
async def verify_payment(req: VerifyPaymentRequest, user=Depends(get_current_user), db=Depends(get_db)):
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode(),
        f"{req.razorpay_order_id}|{req.razorpay_payment_id}".encode(),
        hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, req.razorpay_signature):
        raise HTTPException(status_code=400, detail="Payment signature invalid!")
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {"$set": {"plan": "pro", "razorpay_payment_id": req.razorpay_payment_id,
                  "updated_at": datetime.utcnow()}}
    )
    return {"success": True, "message": "🎉 Pro plan activate ho gaya!"}


@router.get("/status")
async def plan_status(user=Depends(get_current_user)):
    plan = user.get("plan", "free")
    limits = {
        "free": {"files_per_month": 5,  "max_rows": 1000,   "cloud": False, "pdf": False},
        "pro":  {"files_per_month": -1, "max_rows": -1,     "cloud": True,  "pdf": True},
    }
    return {"plan": plan, "files_used_month": user.get("files_used_month", 0), "limits": limits[plan]}


@router.post("/cancel")
async def cancel_plan(user=Depends(get_current_user), db=Depends(get_db)):
    if user.get("plan") != "pro":
        raise HTTPException(status_code=400, detail="Free plan pe ho already")
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {"$set": {"plan": "free", "razorpay_payment_id": None, "updated_at": datetime.utcnow()}}
    )
    return {"message": "Plan cancel ho gaya"}
