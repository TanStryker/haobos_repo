import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, get_db, require_admin
from hrms.core.security import hash_password
from hrms.storage.json_db import JsonDB, now_iso


router = APIRouter(tags=["system"])


@router.get("/admin/users")
def admin_list_users(admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[JsonDB, Depends(get_db)]):
    users = db.read_all("users")
    users.sort(key=lambda u: u.get("user_id", ""))
    out = []
    for u in users:
        out.append(
            {
                "user_id": u.get("user_id", ""),
                "role": u.get("role", "employee"),
                "active": bool(u.get("active", True)),
                "created_at": u.get("created_at", ""),
                "updated_at": u.get("updated_at", ""),
                "must_change_password": bool(u.get("must_change_password", False)),
            }
        )
    return {"items": out}


class UserCreateIn(BaseModel):
    user_id: str = Field(min_length=1)
    role: str = Field(min_length=1, default="employee")
    password: str = Field(min_length=6)


@router.post("/admin/users", status_code=status.HTTP_201_CREATED)
def admin_create_user(payload: UserCreateIn, admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[JsonDB, Depends(get_db)]):
    exists = db.find_one("users", lambda u: u.get("user_id") == payload.user_id)
    if exists:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="账号已存在")
    row = {
        "user_id": payload.user_id,
        "role": payload.role,
        "password_hash": hash_password(payload.password),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "must_change_password": False,
        "active": True,
    }
    db.insert("users", row)
    return {"ok": True}


@router.delete("/admin/users/{user_id}")
def admin_delete_user(
    user_id: str,
    confirm: bool = Query(default=False),
    admin: Annotated[AuthUser, Depends(require_admin)] = None,
    db: Annotated[JsonDB, Depends(get_db)] = None,
):
    if not confirm:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="需要确认删除")
    if user_id == "admin":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不允许删除默认管理员")
    ok = db.delete_one("users", lambda u: u.get("user_id") == user_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="账号不存在")
    return {"ok": True}


class ResetPasswordIn(BaseModel):
    new_password: str = Field(min_length=6)


@router.post("/admin/users/{user_id}/reset-password")
def admin_reset_password(
    user_id: str,
    payload: ResetPasswordIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    def updater(row: dict) -> dict:
        row["password_hash"] = hash_password(payload.new_password)
        row["updated_at"] = now_iso()
        row["must_change_password"] = True
        return row

    updated = db.update_one("users", lambda u: u.get("user_id") == user_id, updater)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="账号不存在")
    return {"ok": True}


@router.get("/admin/logs/operation")
def admin_tail_operation_logs(
    request: Request,
    tail: int = Query(default=200, ge=1, le=2000),
    admin: Annotated[AuthUser, Depends(require_admin)] = None,
):
    log_dir = request.app.state.log_dir
    path = os.path.join(log_dir, "operation_logs.jsonl")
    if not os.path.exists(path):
        return {"items": []}
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    lines = lines[-tail:]
    return {"items": [line.rstrip("\n") for line in lines]}


class ConfigSetIn(BaseModel):
    key: str = Field(min_length=1)
    value: str = Field(min_length=1)


@router.get("/admin/config")
def admin_get_config(admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[JsonDB, Depends(get_db)]):
    items = db.read_all("system_config")
    return {"items": items}


@router.post("/admin/config")
def admin_set_config(payload: ConfigSetIn, admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[JsonDB, Depends(get_db)]):
    existing = db.find_one("system_config", lambda r: r.get("key") == payload.key)
    if not existing:
        db.insert("system_config", {"key": payload.key, "value": payload.value, "updated_at": now_iso(), "updated_by": admin.user_id})
        return {"ok": True}

    def updater(row: dict) -> dict:
        row["value"] = payload.value
        row["updated_at"] = now_iso()
        row["updated_by"] = admin.user_id
        return row

    db.update_one("system_config", lambda r: r.get("key") == payload.key, updater)
    return {"ok": True}

