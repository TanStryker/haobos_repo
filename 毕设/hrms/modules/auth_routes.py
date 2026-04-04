from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, SessionStore, authenticate, get_db, get_sessions, require_user
from hrms.core.security import hash_password, verify_password
from hrms.storage.sqlite_db import SQLiteDB, now_iso


router = APIRouter(prefix="/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=False)


class LoginIn(BaseModel):
    user_id: str = Field(min_length=1)
    password: str = Field(min_length=1)


class LoginOut(BaseModel):
    token: str
    user_id: str
    role: str
    must_change_password: bool = False


@router.post("/login", response_model=LoginOut)
def login(payload: LoginIn, db: Annotated[SQLiteDB, Depends(get_db)], sessions: Annotated[SessionStore, Depends(get_sessions)]):
    user = authenticate(db, payload.user_id, payload.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="账号或密码错误")
    sess = sessions.create(user_id=user["user_id"], role=user.get("role", "employee"))
    return LoginOut(
        token=sess["token"],
        user_id=user["user_id"],
        role=user.get("role", "employee"),
        must_change_password=bool(user.get("must_change_password", False)),
    )


@router.post("/logout")
def logout(
    user: Annotated[AuthUser, Depends(require_user)],
    sessions: Annotated[SessionStore, Depends(get_sessions)],
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)] = None,
):
    if creds is None or not creds.credentials:
        return {"ok": True}
    sessions.delete(creds.credentials)
    return {"ok": True}


@router.get("/me")
def me(user: Annotated[AuthUser, Depends(require_user)]):
    return {"user_id": user.user_id, "role": user.role}


class ChangePasswordIn(BaseModel):
    old_password: str = Field(min_length=1)
    new_password: str = Field(min_length=6)


@router.post("/change-password")
def change_password(payload: ChangePasswordIn, user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    db_user = db.find_one("users", lambda u: u.get("user_id") == user.user_id and u.get("active", True))
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="账号不存在")
    if not verify_password(payload.old_password, db_user.get("password_hash", "")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="原密码错误")
    new_hash = hash_password(payload.new_password)

    def updater(row: dict) -> dict:
        row["password_hash"] = new_hash
        row["updated_at"] = now_iso()
        row["must_change_password"] = False
        return row

    updated = db.update_one("users", lambda u: u.get("user_id") == user.user_id, updater)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="账号不存在")
    return {"ok": True}
