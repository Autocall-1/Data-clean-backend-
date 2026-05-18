"""
AI Analysis Router (Production)
================================
POST /api/analyse/run        — CSV ka AI analysis karo (Pro only)
GET  /api/analyse/{file_id}  — Saved analysis fetch karo

Fixes:
  1. Plan check — free users block kiya (pehle koi check nahi tha)
  2. Gemini API key — URL query param se hatakar header mein (log leak fix)
  3. subscription_status check — halted user bhi block hoga
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
from bson import ObjectId
import httpx, os, json, re, logging

from app.database import get_db
from app.routers.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

# ── AI Provider Config ────────────────────────────────────
AI_PROVIDER    = os.getenv("AI_PROVIDER", "claude")
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Valid subscription states for Pro features
ACTIVE_SUBSCRIPTION_STATES = {"authenticated", "active", None}


def _is_pro_active(user: dict) -> bool:
    if user.get("plan") != "pro":
        return False
    return user.get("subscription_status") in ACTIVE_SUBSCRIPTION_STATES


# ── Schema ────────────────────────────────────────────────
class AnalyseRequest(BaseModel):
    file_id:    str
    csv_sample: str
    col_stats:  dict
    total_rows: int
    total_cols: int
    file_name:  str


# ── Run Analysis (Pro only) ───────────────────────────────
@router.post("/run")
async def run_analysis(
    req: AnalyseRequest,
    db=Depends(get_db),
    user=Depends(get_current_user),
):
    # ── Plan gate — pehle koi check nahi tha ──
    if not _is_pro_active(user):
        raise HTTPException(
            status_code=403,
            detail="AI Analysis Pro plan mein available hai. Upgrade karo!"
        )

    # ── File ownership verify ─────────────────────────────
    try:
        f = await db.file_records.find_one(
            {"_id": ObjectId(req.file_id), "user_firebase_uid": user["firebase_uid"]}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")
    if not f:
        raise HTTPException(status_code=404, detail="File not found")

    # ── AI call ───────────────────────────────────────────
    prompt = _build_prompt(req)
    try:
        analysis = await _call_ai(prompt)
    except Exception as e:
        logger.error("AI API error: provider=%s, error=%s", AI_PROVIDER, str(e))
        raise HTTPException(status_code=502, detail=f"AI API error: {str(e)}")

    parsed = _parse_response(analysis)

    # ── Save to MongoDB ───────────────────────────────────
    now = datetime.now(timezone.utc)
    doc = {
        "file_id":           req.file_id,
        "user_firebase_uid": user["firebase_uid"],
        "file_name":         req.file_name,
        "total_rows":        req.total_rows,
        "total_cols":        req.total_cols,
        "ai_provider":       AI_PROVIDER,
        "raw_response":      analysis,
        "parsed":            parsed,
        "created_at":        now,
    }
    result = await db.analyses.insert_one(doc)

    await db.file_records.update_one(
        {"_id": ObjectId(req.file_id)},
        {"$set": {"has_analysis": True, "analysis_id": str(result.inserted_id)}}
    )

    return {
        "analysis_id": str(result.inserted_id),
        "parsed":      parsed,
        "raw":         analysis,
    }


# ── Fetch saved analysis ──────────────────────────────────
@router.get("/{file_id}")
async def get_analysis(
    file_id: str,
    db=Depends(get_db),
    user=Depends(get_current_user),
):
    doc = await db.analyses.find_one(
        {"file_id": file_id, "user_firebase_uid": user["firebase_uid"]}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Analysis not found")
    return {
        "analysis_id": str(doc["_id"]),
        "parsed":      doc.get("parsed", {}),
        "raw":         doc.get("raw_response", ""),
        "created_at":  doc["created_at"].isoformat() if doc.get("created_at") else None,
    }


# ── Prompt Builder ────────────────────────────────────────
def _build_prompt(req: AnalyseRequest) -> str:
    stats_text = ""
    for col, stat in req.col_stats.items():
        stats_text += f"\n  - {col}: dtype={stat.get('dtype','?')}, nulls={stat.get('null_count',0)}, unique={stat.get('unique_count',0)}"
        if stat.get("mean") is not None:
            stats_text += f", mean={stat.get('mean',0):.2f}, min={stat.get('min',0):.2f}, max={stat.get('max',0):.2f}"
        if stat.get("top_values"):
            stats_text += f", top_values={stat.get('top_values')}"

    return f"""You are a senior data analyst. Analyse this CSV dataset and provide actionable insights.

FILE: {req.file_name}
ROWS: {req.total_rows} | COLUMNS: {req.total_cols}

COLUMN STATISTICS:{stats_text}

DATA SAMPLE (first 5 rows):
{req.csv_sample}

Provide analysis in this EXACT JSON format (no markdown, pure JSON):
{{
  "data_quality_score": <0-100>,
  "summary": "<2-3 sentence summary>",
  "key_findings": ["<finding 1>", "<finding 2>", "<finding 3>", "<finding 4>", "<finding 5>"],
  "column_insights": [
    {{"column": "<name>", "type": "<numeric|categorical|date|text|id>", "health": "<good|warning|critical>", "insight": "<insight>", "recommendation": "<recommendation>"}}
  ],
  "anomalies": [
    {{"type": "<outlier|inconsistency|duplicate_pattern|missing_pattern|format_issue>", "description": "<description>", "severity": "<low|medium|high>", "columns_affected": ["<col>"]}}
  ],
  "business_recommendations": [
    {{"priority": "<high|medium|low>", "title": "<title>", "description": "<description>", "impact": "<impact>"}}
  ],
  "patterns_detected": [
    {{"pattern": "<name>", "description": "<description>", "columns": ["<col>"]}}
  ],
  "next_steps": ["<step 1>", "<step 2>", "<step 3>"]
}}"""


# ── AI Callers ────────────────────────────────────────────
async def _call_ai(prompt: str) -> str:
    provider = AI_PROVIDER.lower()
    if provider == "claude":
        return await _call_claude(prompt)
    elif provider == "openai":
        return await _call_openai(prompt)
    elif provider == "gemini":
        return await _call_gemini(prompt)
    raise ValueError(f"Unknown AI provider: {provider}")


async def _call_claude(prompt: str) -> str:
    if not CLAUDE_API_KEY:
        raise ValueError("CLAUDE_API_KEY env var set nahi hai")
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":    "claude-3-5-haiku-20241022",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
    if res.status_code != 200:
        raise ValueError(f"Claude API error {res.status_code}: {res.text}")
    return res.json()["content"][0]["text"]


async def _call_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY env var set nahi hai")
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model":      "gpt-4o-mini",
                "max_tokens": 2000,
                "messages":   [{"role": "user", "content": prompt}],
            },
        )
    if res.status_code != 200:
        raise ValueError(f"OpenAI API error {res.status_code}: {res.text}")
    return res.json()["choices"][0]["message"]["content"]


async def _call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY env var set nahi hai")
    # Fix: key URL mein nahi, header mein — logs mein leak nahi hogi
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent",
            headers={
                "x-goog-api-key": GEMINI_API_KEY,
                "Content-Type":   "application/json",
            },
            json={"contents": [{"parts": [{"text": prompt}]}]},
        )
    if res.status_code != 200:
        raise ValueError(f"Gemini API error {res.status_code}: {res.text}")
    return res.json()["candidates"][0]["content"]["parts"][0]["text"]


# ── Response Parser ───────────────────────────────────────
def _parse_response(raw: str) -> dict:
    for attempt in [
        lambda: json.loads(raw),
        lambda: json.loads(re.search(r'```(?:json)?\s*([\s\S]*?)```', raw).group(1)),
        lambda: json.loads(re.search(r'\{[\s\S]*\}', raw).group()),
    ]:
        try:
            return attempt()
        except Exception:
            pass
    return {
        "data_quality_score": 0,
        "summary":            raw[:300],
        "key_findings":       ["Analysis parse nahi ho saka"],
        "column_insights":    [], "anomalies": [],
        "business_recommendations": [], "patterns_detected": [],
        "next_steps":         ["Raw response check karo"],
    }
