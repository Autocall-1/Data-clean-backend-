"""
Plans Router — Razorpay SUBSCRIPTION Model (Production)
========================================================

Endpoints:
  POST /api/plans/create-subscription   — Razorpay subscription banao
  POST /api/plans/verify-subscription   — Payment verify + user upgrade
  POST /api/plans/webhook               — Razorpay events (auto-cancel, renewal)
  GET  /api/plans/status                — Current plan + limits
  POST /api/plans/cancel                — Subscription cancel karo

Razorpay Subscription flow:
  1. Frontend: create-subscription call → subscription_id + payment_link milta hai
  2. User pays on Razorpay Checkout
  3. Frontend: verify-subscription call → signature verify → Pro activate
  4. Razorpay auto-renews monthly → webhook se DB update
  5. Payment fail / cancel → webhook se plan free kar do

NOTE: Razorpay Dashboard pe pehle ek "Plan" banana padega:
  - Period: monthly
  - Interval: 1
  - Amount: 49900 paise (₹499)
  - Copy karo RAZORPAY_PLAN_ID → env var mein dalo
"""

import hmac
import hashlib
import base64
import os
import logging
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Header
from pydantic import BaseModel
from typing import Optional

from app.database import get_db
from app.routers.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Config ────────────────────────────────────────────────
RAZORPAY_KEY_ID      = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET  = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_PLAN_ID     = os.getenv("RAZORPAY_PLAN_ID", "")       # Razorpay dashboard se
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")  # Webhook pe set karo

PLAN_LIMITS = {
    "free": {"files_per_month": 5,  "max_rows": 1_000,  "cloud": False, "pdf": False, "ai_analysis": False},
    "pro":  {"files_per_month": -1, "max_rows": -1,     "cloud": True,  "pdf": True,  "ai_analysis": True},
}


def _razorpay_auth() -> str:
    """Basic auth header value banao."""
    return base64.b64encode(
        f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode()
    ).decode()


def _verify_razorpay_signature(payload: str, signature: str, secret: str) -> bool:
    """
    Razorpay HMAC-SHA256 signature verify karo.
    payload = "subscription_id|payment_id"  (subscription payment ke liye)
             = raw webhook body             (webhook ke liye)
    """
    expected = hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Schemas ───────────────────────────────────────────────
class VerifySubscriptionRequest(BaseModel):
    razorpay_payment_id:    str
    razorpay_subscription_id: str
    razorpay_signature:     str


# ── 1. Create Subscription ────────────────────────────────
@router.post("/create-subscription")
async def create_subscription(
    user=Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Razorpay pe monthly subscription banao.
    Frontend isko subscription_id milta hai, phir Checkout open karta hai.
    """
    if not RAZORPAY_PLAN_ID:
        raise HTTPException(status_code=500, detail="RAZORPAY_PLAN_ID env var set nahi hai")

    # Already active subscription hai toh block karo
    if user.get("plan") == "pro" and user.get("razorpay_subscription_id"):
        sub_status = user.get("subscription_status", "")
        if sub_status in ("created", "authenticated", "active"):
            raise HTTPException(
                status_code=400,
                detail=f"Pehle se active subscription hai (status: {sub_status}). Cancel karo pehle."
            )

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                "https://api.razorpay.com/v1/subscriptions",
                headers={"Authorization": f"Basic {_razorpay_auth()}"},
                json={
                    "plan_id":        RAZORPAY_PLAN_ID,
                    "total_count":    120,          # max 10 saal — practically infinite
                    "quantity":       1,
                    "customer_notify": 1,           # Razorpay khud email bhejega
                    "notes": {
                        "firebase_uid": user["firebase_uid"],
                        "email":        user["email"],
                        "name":         user.get("name", ""),
                    },
                },
            )
        if res.status_code != 200:
            logger.error("Razorpay subscription create failed: %s", res.text)
            raise HTTPException(status_code=502, detail="Razorpay subscription create nahi hua")

        sub = res.json()
        subscription_id = sub["id"]

        # DB mein pending state save karo (idempotent)
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {
                "$set": {
                    "razorpay_subscription_id": subscription_id,
                    "subscription_status":      "created",
                    "updated_at":               datetime.now(timezone.utc),
                }
            },
        )

        return {
            "subscription_id": subscription_id,
            "razorpay_key":    RAZORPAY_KEY_ID,
            "prefill": {
                "name":    user.get("name", ""),
                "email":   user["email"],
            },
            "amount": 49900,   # frontend display ke liye
            "currency": "INR",
        }

    except httpx.RequestError as e:
        logger.error("Razorpay network error: %s", str(e))
        raise HTTPException(status_code=502, detail=f"Razorpay se connect nahi ho saka: {str(e)}")


# ── 2. Verify Subscription Payment ───────────────────────
@router.post("/verify-subscription")
async def verify_subscription(
    req: VerifySubscriptionRequest,
    user=Depends(get_current_user),
    db=Depends(get_db),
):
    """
    User ne payment kiya → signature verify karo → Pro activate karo.

    Razorpay subscription signature payload:
      razorpay_payment_id + "|" + razorpay_subscription_id
    """
    # ── 1. Signature verify ──
    payload   = f"{req.razorpay_payment_id}|{req.razorpay_subscription_id}"
    if not _verify_razorpay_signature(payload, req.razorpay_signature, RAZORPAY_KEY_SECRET):
        logger.warning(
            "Signature mismatch: uid=%s, sub=%s, pay=%s",
            user["firebase_uid"], req.razorpay_subscription_id, req.razorpay_payment_id,
        )
        raise HTTPException(status_code=400, detail="Payment signature invalid hai!")

    # ── 2. Idempotency — same payment_id se dobara process mat karo ──
    already = await db.users.find_one(
        {"razorpay_last_payment_id": req.razorpay_payment_id}
    )
    if already:
        return {"success": True, "message": "Payment pehle se process ho chuki hai", "already_active": True}

    # ── 3. Razorpay se subscription ka live status confirm karo ──
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(
                f"https://api.razorpay.com/v1/subscriptions/{req.razorpay_subscription_id}",
                headers={"Authorization": f"Basic {_razorpay_auth()}"},
            )
        if res.status_code != 200:
            raise HTTPException(status_code=502, detail="Razorpay se subscription verify nahi ho saka")

        sub_data = res.json()
        rzp_status = sub_data.get("status", "")

        # Sirf authenticated ya active status pe Pro dena — created/halted pe nahi
        if rzp_status not in ("authenticated", "active"):
            raise HTTPException(
                status_code=400,
                detail=f"Subscription abhi active nahi hai (Razorpay status: {rzp_status})"
            )

    except httpx.RequestError as e:
        # Network error pe local signature se trust karo (rare production case)
        logger.error("Razorpay verify fetch failed: %s", str(e))
        rzp_status = "active"   # signature already verified hai upar

    # ── 4. User ko Pro upgrade karo ──
    now = datetime.now(timezone.utc)
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {
            "$set": {
                "plan":                     "pro",
                "subscription_status":      rzp_status,
                "razorpay_subscription_id": req.razorpay_subscription_id,
                "razorpay_last_payment_id": req.razorpay_payment_id,
                "pro_since":                now,
                "updated_at":               now,
            }
        },
    )

    logger.info(
        "Pro activated: uid=%s, sub=%s",
        user["firebase_uid"], req.razorpay_subscription_id,
    )
    return {"success": True, "message": "Pro plan activate ho gaya!"}


# ── 3. Webhook — Razorpay events handle karo ─────────────
@router.post("/webhook")
async def razorpay_webhook(
    request: Request,
    db=Depends(get_db),
    x_razorpay_signature: Optional[str] = Header(None),
):
    """
    Razorpay Dashboard pe yeh URL set karo:
      https://your-api.com/api/plans/webhook

    Events handled:
      subscription.activated    — renewal ya first activation
      subscription.charged      — monthly payment successful
      subscription.halted       — multiple payment failures
      subscription.cancelled    — user ya admin ne cancel kiya
      subscription.completed    — total_count khatam (120 months baad)
      payment.failed            — single payment fail (halt se pehle)
    """
    raw_body = await request.body()

    # ── Webhook signature verify (RAZORPAY_WEBHOOK_SECRET se) ──
    if RAZORPAY_WEBHOOK_SECRET:
        if not x_razorpay_signature:
            logger.warning("Webhook: signature header missing")
            raise HTTPException(status_code=400, detail="Signature header missing")
        if not _verify_razorpay_signature(
            raw_body.decode("utf-8"), x_razorpay_signature, RAZORPAY_WEBHOOK_SECRET
        ):
            logger.warning("Webhook: signature invalid")
            raise HTTPException(status_code=400, detail="Webhook signature invalid")

    try:
        import json
        payload = json.loads(raw_body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    event      = payload.get("event", "")
    entity     = payload.get("payload", {}).get("subscription", {}).get("entity", {})
    sub_id     = entity.get("id", "")
    rzp_status = entity.get("status", "")

    logger.info("Webhook received: event=%s, sub=%s, status=%s", event, sub_id, rzp_status)

    if not sub_id:
        return {"status": "ignored", "reason": "no subscription id"}

    now = datetime.now(timezone.utc)

    # ── Event handlers ──
    if event == "subscription.charged":
        # Monthly renewal successful
        payment_entity = payload.get("payload", {}).get("payment", {}).get("entity", {})
        payment_id     = payment_entity.get("id", "")
        await db.users.update_one(
            {"razorpay_subscription_id": sub_id},
            {
                "$set": {
                    "plan":                     "pro",
                    "subscription_status":      "active",
                    "razorpay_last_payment_id": payment_id,
                    "last_renewal_at":          now,
                    "updated_at":               now,
                }
            },
        )
        logger.info("Subscription renewed: sub=%s, payment=%s", sub_id, payment_id)

    elif event in ("subscription.activated",):
        await db.users.update_one(
            {"razorpay_subscription_id": sub_id},
            {"$set": {"plan": "pro", "subscription_status": "active", "updated_at": now}},
        )

    elif event in ("subscription.halted", "subscription.cancelled", "subscription.completed"):
        # Plan downgrade karo
        await db.users.update_one(
            {"razorpay_subscription_id": sub_id},
            {
                "$set": {
                    "plan":                "free",
                    "subscription_status": rzp_status,
                    "updated_at":          now,
                }
            },
        )
        logger.info("Plan downgraded to free: sub=%s, event=%s", sub_id, event)

    elif event == "payment.failed":
        # Sirf log karo, plan abhi mat girао — Razorpay retry karega
        payment_entity = payload.get("payload", {}).get("payment", {}).get("entity", {})
        await db.users.update_one(
            {"razorpay_subscription_id": sub_id},
            {
                "$set": {
                    "subscription_status":  "payment_failed",
                    "last_payment_error":   payment_entity.get("error_description", ""),
                    "updated_at":           now,
                }
            },
        )
        logger.warning("Payment failed: sub=%s", sub_id)

    return {"status": "ok", "event": event}


# ── 4. Plan Status ────────────────────────────────────────
@router.get("/status")
async def plan_status(user=Depends(get_current_user)):
    plan   = user.get("plan", "free")
    limits = PLAN_LIMITS[plan]

    return {
        "plan":                     plan,
        "subscription_status":      user.get("subscription_status"),
        "razorpay_subscription_id": user.get("razorpay_subscription_id"),
        "files_used_month":         user.get("files_used_month", 0),
        "pro_since":                user["pro_since"].isoformat() if user.get("pro_since") else None,
        "last_renewal_at":          user["last_renewal_at"].isoformat() if user.get("last_renewal_at") else None,
        "limits":                   limits,
    }


# ── 5. Cancel Subscription ────────────────────────────────
@router.post("/cancel")
async def cancel_subscription(
    user=Depends(get_current_user),
    db=Depends(get_db),
):
    """
    Razorpay subscription cancel karo.
    cancel_at_cycle_end=1 → current billing period khatam hone ke baad cancel hoga.
    Webhook (subscription.cancelled) aane pe plan free ho jaayega automatically.
    """
    if user.get("plan") != "pro":
        raise HTTPException(status_code=400, detail="Tum pehle se free plan pe ho")

    sub_id = user.get("razorpay_subscription_id")
    if not sub_id:
        # Orphan case — DB mein pro hai lekin subscription_id nahi
        await db.users.update_one(
            {"firebase_uid": user["firebase_uid"]},
            {"$set": {"plan": "free", "subscription_status": "cancelled", "updated_at": datetime.now(timezone.utc)}},
        )
        return {"message": "Plan cancel ho gaya (local only — no active Razorpay subscription found)"}

    # ── Razorpay pe cancel karo ──
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                f"https://api.razorpay.com/v1/subscriptions/{sub_id}/cancel",
                headers={"Authorization": f"Basic {_razorpay_auth()}"},
                json={"cancel_at_cycle_end": 1},  # 0 = abhi cancel, 1 = period end pe
            )

        if res.status_code not in (200, 201):
            rzp_error = res.json()
            logger.error("Razorpay cancel failed: %s", rzp_error)
            raise HTTPException(
                status_code=502,
                detail=f"Razorpay cancel nahi hua: {rzp_error.get('error', {}).get('description', 'Unknown error')}"
            )

        rzp_data   = res.json()
        rzp_status = rzp_data.get("status", "cancelled")

    except httpx.RequestError as e:
        logger.error("Razorpay cancel network error: %s", str(e))
        raise HTTPException(status_code=502, detail=f"Razorpay se connect nahi ho saka: {str(e)}")

    # ── DB update — plan abhi pro rehega jab tak webhook nahi aata ──
    await db.users.update_one(
        {"firebase_uid": user["firebase_uid"]},
        {
            "$set": {
                "subscription_status": rzp_status,   # "cancelled" ya "active" (cancel_at_cycle_end)
                "updated_at":          datetime.now(timezone.utc),
            }
        },
    )

    message = (
        "Subscription cancel schedule ho gaya — current billing period khatam hone ke baad Pro access band hoga."
        if rzp_status == "active"
        else "Subscription abhi cancel ho gaya."
    )
    return {"message": message, "razorpay_status": rzp_status}
