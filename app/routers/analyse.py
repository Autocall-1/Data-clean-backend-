"""
AI Analysis Router
==================
POST /api/analyse/run        — CSV ka AI analysis karo
GET  /api/analyse/{file_id}  — Saved analysis fetch karo

AI Provider flexible hai — env var se switch karo:
  AI_PROVIDER = claude | openai | gemini
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from bson import ObjectId
import httpx, os, json, re

from app.database import get_db
from app.routers.auth import get_current_user

router = APIRouter()

# ── AI Provider Config ────────────────────────────────────
AI_PROVIDER      = os.getenv("AI_PROVIDER", "claude")          # claude | openai | gemini
CLAUDE_API_KEY   = os.getenv("CLAUDE_API_KEY", "")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")


# ── Schema ───────────────────────────────────────────────
class AnalyseRequest(BaseModel):
    file_id:     str
    csv_sample:  str        # First 100 rows ka CSV text
    col_stats:   dict       # Column statistics
    total_rows:  int
    total_cols:  int
    file_name:   str


# ── Main endpoint ─────────────────────────────────────────
@router.post("/run")
async def run_analysis(
    req: AnalyseRequest,
    db=Depends(get_db),
    user=Depends(get_current_user)
):
    # Verify file ownership
    try:
        f = await db.file_records.find_one(
            {"_id": ObjectId(req.file_id), "user_firebase_uid": user["firebase_uid"]}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file ID")
    if not f:
        raise HTTPException(status_code=404, detail="File not found")

    # Build prompt
    prompt = build_analysis_prompt(req)

    # Call AI
    try:
        analysis = await call_ai(prompt)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"AI API error: {str(e)}")

    # Parse structured response
    parsed = parse_ai_response(analysis)

    # Save to MongoDB
    now = datetime.utcnow()
    analysis_doc = {
        "file_id":          req.file_id,
        "user_firebase_uid": user["firebase_uid"],
        "file_name":        req.file_name,
        "total_rows":       req.total_rows,
        "total_cols":       req.total_cols,
        "ai_provider":      AI_PROVIDER,
        "raw_response":     analysis,
        "parsed":           parsed,
        "created_at":       now,
    }
    result = await db.analyses.insert_one(analysis_doc)

    # Also update file record
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
    user=Depends(get_current_user)
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
def build_analysis_prompt(req: AnalyseRequest) -> str:
    stats_text = ""
    for col, stat in req.col_stats.items():
        stats_text += f"\n  - {col}: dtype={stat.get('dtype','?')}, "
        stats_text += f"nulls={stat.get('null_count',0)}, "
        stats_text += f"unique={stat.get('unique_count',0)}"
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

Provide a comprehensive analysis in this EXACT JSON format (no markdown, pure JSON):
{{
  "data_quality_score": <0-100 number>,
  "summary": "<2-3 sentence overall summary of what this dataset contains and its quality>",
  "key_findings": [
    "<finding 1>",
    "<finding 2>",
    "<finding 3>",
    "<finding 4>",
    "<finding 5>"
  ],
  "column_insights": [
    {{
      "column": "<col name>",
      "type": "<numeric|categorical|date|text|id>",
      "health": "<good|warning|critical>",
      "insight": "<specific insight about this column>",
      "recommendation": "<what to do with this column>"
    }}
  ],
  "anomalies": [
    {{
      "type": "<outlier|inconsistency|duplicate_pattern|missing_pattern|format_issue>",
      "description": "<what anomaly was found>",
      "severity": "<low|medium|high>",
      "columns_affected": ["<col1>"]
    }}
  ],
  "business_recommendations": [
    {{
      "priority": "<high|medium|low>",
      "title": "<short title>",
      "description": "<actionable recommendation>",
      "impact": "<what business impact this has>"
    }}
  ],
  "patterns_detected": [
    {{
      "pattern": "<pattern name>",
      "description": "<what pattern was detected>",
      "columns": ["<col1>", "<col2>"]
    }}
  ],
  "next_steps": [
    "<step 1>",
    "<step 2>",
    "<step 3>"
  ]
}}

Be specific, data-driven, and actionable. Focus on insights that would genuinely help a business user understand and use this data better."""


# ── AI API Callers ────────────────────────────────────────
async def call_ai(prompt: str) -> str:
    provider = AI_PROVIDER.lower()
    if provider == "claude":
        return await call_claude(prompt)
    elif provider == "openai":
        return await call_openai(prompt)
    elif provider == "gemini":
        return await call_gemini(prompt)
    else:
        raise ValueError(f"Unknown AI provider: {provider}")


async def call_claude(prompt: str) -> str:
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
                "model":      "claude-3-5-haiku-20241022",
                "max_tokens": 2000,
                "messages":   [{"role": "user", "content": prompt}]
            }
        )
    if res.status_code != 200:
        raise ValueError(f"Claude API error: {res.text}")
    data = res.json()
    return data["content"][0]["text"]


async def call_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY env var set nahi hai")
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model":       "gpt-4o-mini",
                "max_tokens":  2000,
                "messages":    [{"role": "user", "content": prompt}]
            }
        )
    if res.status_code != 200:
        raise ValueError(f"OpenAI API error: {res.text}")
    return res.json()["choices"][0]["message"]["content"]


async def call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY env var set nahi hai")
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]}
        )
    if res.status_code != 200:
        raise ValueError(f"Gemini API error: {res.text}")
    return res.json()["candidates"][0]["content"]["parts"][0]["text"]


# ── Response Parser ───────────────────────────────────────
def parse_ai_response(raw: str) -> dict:
    """Extract JSON from AI response"""
    try:
        # Direct JSON parse
        return json.loads(raw)
    except Exception:
        pass
    try:
        # Extract JSON from markdown code block
        match = re.search(r'```(?:json)?\s*([\s\S]*?)```', raw)
        if match:
            return json.loads(match.group(1))
    except Exception:
        pass
    try:
        # Find first { ... } block
        match = re.search(r'\{[\s\S]*\}', raw)
        if match:
            return json.loads(match.group())
    except Exception:
        pass
    # Fallback
    return {
        "data_quality_score": 0,
        "summary":            raw[:300],
        "key_findings":       ["Analysis parse nahi ho saka"],
        "column_insights":    [],
        "anomalies":          [],
        "business_recommendations": [],
        "patterns_detected":  [],
        "next_steps":         ["Raw response check karo"]
    }
