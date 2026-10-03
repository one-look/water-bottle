import os
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def _ensure_test_auth_env() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")
    fallbacks = {
        "PUBLIC_BASE_URL": "http://test.example.com",
        "AUTH_PRIVATE_KEY": private_pem,
        "AUTH_PUBLIC_KEY": public_pem,
        "AUTH_WATER_BOTTLE_CLIENT_SECRET": "test-client-secret",
    }
    for key, value in fallbacks.items():
        if not os.environ.get(key):
            os.environ[key] = value


_ensure_test_auth_env()

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
