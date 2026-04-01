import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def hash_password(password: str, salt: str | None = None) -> str:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    return f"pbkdf2_sha256${salt}${dk.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, salt, digest = password_hash.split("$", 2)
        if scheme != "pbkdf2_sha256":
            return False
    except ValueError:
        return False
    expected = hash_password(password, salt=salt)
    return hmac.compare_digest(expected, password_hash)


def new_token() -> str:
    return secrets.token_urlsafe(32)


def default_session_expiry() -> datetime:
    return now_utc() + timedelta(minutes=30)

