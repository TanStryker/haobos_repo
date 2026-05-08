from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from hrms.core.security import default_session_expiry, new_token, now_utc, verify_password
from hrms.storage.sqlite_db import SQLiteDB, now_iso


@dataclass(frozen=True)
class AuthUser:
    user_id: str
    role: str


_bearer = HTTPBearer(auto_error=False)


class SessionStore:
    def __init__(self, db: SQLiteDB):
        self._db = db

    def create(self, user_id: str, role: str, tab_id: str) -> dict:
        token = new_token()
        expires_at = default_session_expiry()
        sess = {"token": token, "user_id": user_id, "role": role, "expires_at": expires_at.isoformat()}
        safe_tab = str(tab_id or "").strip() or "-"
        self._db.upsert_session(user_id=user_id, token=token, role=role, expires_at=sess["expires_at"], tab_id=safe_tab)
        return sess

    def get(self, token: str, tab_id: str) -> dict | None:
        sess = self._db.get_session_by_token(token)
        if not sess:
            return None
        stored_tab = str(sess.get("tab_id", "")).strip()
        req_tab = str(tab_id or "").strip()
        wildcard = {"-", "*"}
        if stored_tab and req_tab and stored_tab not in wildcard and req_tab not in wildcard and stored_tab != req_tab:
            return None
        try:
            expires_at = datetime.fromisoformat(str(sess.get("expires_at", "")))
        except Exception:
            self._db.delete_session_by_token(token)
            return None
        if expires_at < now_utc():
            self._db.delete_session_by_token(token)
            return None
        return sess

    def delete(self, token: str) -> None:
        self._db.delete_session_by_token(token)


def get_db(request: Request) -> SQLiteDB:
    return request.app.state.db


def get_sessions(request: Request) -> SessionStore:
    return request.app.state.sessions


def ensure_default_admin(db: SQLiteDB) -> None:
    users = db.read_all("users")
    if any(u.get("role") == "admin" for u in users):
        return
    from hrms.core.security import hash_password
    if not db.find_one("employees", lambda e: e.get("employee_id") == "admin"):
        db.insert(
            "employees",
            {
                "employee_id": "admin",
                "name": "系统管理员",
                "department": "系统",
                "position": "管理员",
                "work_type": "onsite",
                "daily_salary": 0,
                "attendance_days": 0,
                "active": True,
                "hire_date": "1970-01-01",
            },
        )
    admin = {
        "user_id": "admin",
        "role": "admin",
        "password_hash": hash_password("admin123"),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "must_change_password": True,
        "active": True,
    }
    db.insert("users", admin)


def authenticate(db: SQLiteDB, user_id: str, password: str) -> dict | None:
    user = db.find_one("users", lambda u: u.get("user_id") == user_id and u.get("active", True))
    if not user:
        return None
    if not verify_password(password, user.get("password_hash", "")):
        return None
    return user


def require_user(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    db: Annotated[SQLiteDB, Depends(get_db)],
    sessions: Annotated[SessionStore, Depends(get_sessions)],
    request: Request,
) -> AuthUser:
    if creds is None or not creds.credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="未登录")
    tab_id = request.headers.get("X-HRMS-Tab", "")
    sess = sessions.get(creds.credentials, tab_id=tab_id)
    if not sess:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="会话无效或已过期")
    user = db.find_one("users", lambda u: u.get("user_id") == sess["user_id"] and u.get("active", True))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="账号不可用")
    return AuthUser(user_id=user["user_id"], role=user.get("role", "employee"))


def require_admin(user: Annotated[AuthUser, Depends(require_user)]) -> AuthUser:
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权限")
    return user

