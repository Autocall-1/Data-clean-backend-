"""
DataClean AI — FastAPI Backend v3.0
Auth: Firebase | DB: MongoDB | Storage: Cloudinary | AI Analysis
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

from app.database import connect_db, disconnect_db
from app.utils.firebase_setup import init_firebase
from app.routers import auth, files, cleaning, plans, cloudinary_router, analyse


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firebase()
    await connect_db()
    yield
    await disconnect_db()


app = FastAPI(
    title="DataClean AI API",
    description="Auth:Firebase | DB:MongoDB | Storage:Cloudinary | AI:Flexible",
    version="3.0.0",
    lifespan=lifespan
)

raw = os.getenv("ALLOWED_ORIGINS", "*")
origins = [o.strip() for o in raw.split(",")]
dev_origins = ["http://localhost:3000","http://localhost:8080","http://127.0.0.1:5500"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(set(origins + dev_origins)),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router,               prefix="/api/auth",       tags=["Auth"])
app.include_router(files.router,              prefix="/api/files",      tags=["Files"])
app.include_router(cleaning.router,           prefix="/api/clean",      tags=["Cleaning"])
app.include_router(analyse.router,            prefix="/api/analyse",    tags=["AI Analysis"])
app.include_router(plans.router,              prefix="/api/plans",      tags=["Plans"])
app.include_router(cloudinary_router.router,  prefix="/api/cloudinary", tags=["Cloudinary"])

@app.get("/")
def root():
    return {"status":"ok","app":"DataClean AI","version":"3.0.0"}

@app.get("/health")
def health():
    return {"status":"healthy"}
