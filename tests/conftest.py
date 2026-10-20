import os
import sys
from pathlib import Path

fallbacks = {
    "PUBLIC_BASE_URL": "http://test.example.com",
    "GOOGLE_OAUTH_CLIENT_ID": "test-google-client-id",
    "GOOGLE_OAUTH_CLIENT_SECRET": "test-google-client-secret",
    "DATABASE_URL": "postgresql://localhost:5432/test",
    "ATTENDANCE_DATABASE_URL": "postgresql://localhost:5432/attendance",
    "WHATSAPP_VERIFY_TOKEN": "test-verify-token",
    "WHATSAPP_PHONE_NUMBER_ID": "000000000000000",
    "WHATSAPP_TOKEN": "test-whatsapp-token",
}
for key, value in fallbacks.items():
    if not os.environ.get(key):
        os.environ[key] = value

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
