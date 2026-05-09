import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, get_db, require_admin, require_user
from hrms.storage.sqlite_db import SQLiteDB, now_iso


router = APIRouter(tags=["overtime"])


class OvertimeCreate(BaseModel):
    start_date: str | None = Field(default=None, min_length=10, max_length=10)
    end_date: str | None = Field(default=None, min_length=10, max_length=10)
    reason: str = Field(min_length=1)
    date: str | None = Field(default=None, min_length=10, max_length=10)
    days: float | None = Field(default=None, gt=0)


def _parse_date_yyyy_mm_dd(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="日期格式错误，应为 YYYY-MM-DD")


def _resolve_range(payload: OvertimeCreate) -> tuple[str, str, float]:
    if payload.start_date and payload.end_date:
        sd = payload.start_date.strip()
        ed = payload.end_date.strip()
        sdt = _parse_date_yyyy_mm_dd(sd)
        edt = _parse_date_yyyy_mm_dd(ed)
        if edt < sdt:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="结束日期不能早于开始日期")
        days = float((edt - sdt).days + 1)
        return sd, ed, days
    if payload.date and payload.days is not None:
        d = payload.date.strip()
        _parse_date_yyyy_mm_dd(d)
        return d, d, float(payload.days)
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请填写加班起止日期")


@router.post("/me/overtime", status_code=status.HTTP_201_CREATED)
def employee_submit_overtime(
    payload: OvertimeCreate,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id and e.get("active", True))
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工信息不存在")
    start_date, end_date, days = _resolve_range(payload)
    row = {
        "id": str(uuid.uuid4()),
        "employee_id": user.user_id,
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "reason": payload.reason,
        "status": "pending",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "approved_by": "",
        "approved_at": "",
        "rejected_reason": "",
    }
    db.insert("overtime_requests", row)
    return row


@router.get("/me/overtime")
def employee_list_overtime(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    items = db.find_many("overtime_requests", lambda r: r.get("employee_id") == user.user_id)
    items.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return {"items": items}


@router.get("/admin/overtime/pending")
def admin_list_pending_overtime(admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[SQLiteDB, Depends(get_db)]):
    items = db.find_many("overtime_requests", lambda r: r.get("status") == "pending")
    items.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return {"items": items}


class RejectIn(BaseModel):
    reason: str = Field(min_length=1)


@router.post("/admin/overtime/{request_id}/approve")
def admin_approve_overtime(
    request_id: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    req = db.find_one("overtime_requests", lambda r: r.get("id") == request_id)
    if not req:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="申请不存在")
    if req.get("status") != "pending":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="申请已处理")

    def updater(row: dict) -> dict:
        row["status"] = "approved"
        row["approved_by"] = admin.user_id
        row["approved_at"] = now_iso()
        row["updated_at"] = now_iso()
        return row

    db.update_one("overtime_requests", lambda r: r.get("id") == request_id, updater)
    return {"ok": True}


@router.post("/admin/overtime/{request_id}/reject")
def admin_reject_overtime(
    request_id: str,
    payload: RejectIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    req = db.find_one("overtime_requests", lambda r: r.get("id") == request_id)
    if not req:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="申请不存在")
    if req.get("status") != "pending":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="申请已处理")

    def updater(row: dict) -> dict:
        row["status"] = "rejected"
        row["approved_by"] = admin.user_id
        row["approved_at"] = now_iso()
        row["rejected_reason"] = payload.reason
        row["updated_at"] = now_iso()
        return row

    db.update_one("overtime_requests", lambda r: r.get("id") == request_id, updater)
    return {"ok": True}


@router.get("/admin/overtime/records")
def admin_list_overtime_records(
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
    status_filter: str | None = Query(default=None, alias="status"),
):
    items = db.read_all("overtime_requests")
    if status_filter:
        items = [r for r in items if r.get("status") == status_filter]
    items.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return {"items": items}


@router.get("/admin/overtime/stats")
def admin_overtime_stats(
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    approved = db.find_many(
        "overtime_requests",
        lambda r: r.get("status") == "approved" and str(r.get("start_date") or r.get("date") or "").startswith(month),
    )
    totals: dict[str, float] = {}
    for r in approved:
        emp_id = r.get("employee_id", "")
        totals[emp_id] = totals.get(emp_id, 0) + float(r.get("days", 0) or 0)
    items = [{"employee_id": k, "overtime_days": v} for k, v in totals.items()]
    items.sort(key=lambda x: x["employee_id"])
    return {"month": month, "items": items}

