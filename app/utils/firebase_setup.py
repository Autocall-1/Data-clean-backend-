"""
Firebase Admin SDK Setup
========================
Backend pe Firebase token verify karne ke liye
"""

import firebase_admin
from firebase_admin import credentials
import os
import json

def init_firebase():
    """
    Firebase Admin SDK initialize karo.
    FIREBASE_SERVICE_ACCOUNT_JSON env var mein JSON string hona chahiye.
    """
    if firebase_admin._apps:
        return  # Already initialized

    # Env var se service account JSON lo
    sa_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

    if sa_json:
        # Render pe env var se
        try:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
        except json.JSONDecodeError:
            raise ValueError("FIREBASE_SERVICE_ACCOUNT_JSON invalid JSON hai")
    else:
        # Local dev ke liye — serviceAccountKey.json file
        key_path = os.getenv("FIREBASE_KEY_PATH", "serviceAccountKey.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(
                f"Firebase key nahi mila: {key_path}\n"
                "Ya FIREBASE_SERVICE_ACCOUNT_JSON env var set karo\n"
                "Ya serviceAccountKey.json file rakho backend folder mein"
            )
        cred = credentials.Certificate(key_path)

    firebase_admin.initialize_app(cred)
    print("✅ Firebase Admin SDK initialized!")
