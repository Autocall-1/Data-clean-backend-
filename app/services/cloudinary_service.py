"""Cloudinary Service — CSV Upload/Delete"""
import httpx, base64, time, hashlib, os
from typing import Optional

async def upload_csv_to_cloudinary(content: bytes, filename: str, cloud_name: str, upload_preset: str, folder: str = "dataclean") -> Optional[str]:
    try:
        url = f"https://api.cloudinary.com/v1_1/{cloud_name}/raw/upload"
        b64 = base64.b64encode(content).decode("utf-8")
        payload = {
            "file": f"data:text/csv;base64,{b64}",
            "upload_preset": upload_preset,
            "folder": folder,
            "public_id": f"{filename.replace('.csv','').replace(' ','_')}_{int(time.time())}",
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            res = await client.post(url, data=payload)
            if res.status_code == 200:
                return res.json().get("secure_url")
    except Exception as e:
        print(f"Cloudinary upload error: {e}")
    return None

async def delete_from_cloudinary(secure_url: str, cloud_name: str) -> bool:
    try:
        key    = os.getenv("CLOUDINARY_API_KEY", "")
        secret = os.getenv("CLOUDINARY_API_SECRET", "")
        if not key or not secret: return False
        parts = secure_url.split("/upload/")
        if len(parts) < 2: return False
        pid = "/".join(parts[1].split("/")[1:])
        pid = pid.rsplit(".", 1)[0] if "." in pid else pid
        ts  = int(time.time())
        sig = hashlib.sha1(f"public_id={pid}&timestamp={ts}{secret}".encode()).hexdigest()
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                f"https://api.cloudinary.com/v1_1/{cloud_name}/raw/destroy",
                data={"public_id": pid, "timestamp": ts, "api_key": key, "signature": sig}
            )
            return res.status_code == 200
    except Exception as e:
        print(f"Cloudinary delete error: {e}")
        return False
