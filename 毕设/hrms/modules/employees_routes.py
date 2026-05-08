import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, get_db, require_admin, require_user
from hrms.storage.sqlite_db import SQLiteDB, now_iso


router = APIRouter(tags=["employees"])


class EmployeeBase(BaseModel):
    employee_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    department: str = Field(min_length=1)
    position: str = Field(min_length=1)
    work_type: str = Field(min_length=1, default="onsite")
    daily_salary: float = Field(ge=0)
    attendance_days: float = Field(ge=0, default=0)
    active: bool = True


class EmployeeCreate(BaseModel):
    employee_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    department: str = Field(min_length=1)
    position: str = Field(min_length=1)
    work_type: str = Field(min_length=1, default="onsite")
    daily_salary: float = Field(ge=0)
    attendance_days: float = Field(ge=0, default=0)


class EmployeeUpdate(BaseModel):
    name: str | None = None
    department: str | None = None
    position: str | None = None
    work_type: str | None = None
    daily_salary: float | None = Field(default=None, ge=0)
    attendance_days: float | None = Field(default=None, ge=0)
    active: bool | None = None


def _employee_public(row: dict) -> dict:
    return {
        "employee_id": row.get("employee_id", ""),
        "name": row.get("name", ""),
        "department": row.get("department", ""),
        "position": row.get("position", ""),
        "work_type": row.get("work_type", "onsite"),
        "daily_salary": row.get("daily_salary", 0),
        "attendance_days": row.get("attendance_days", 0),
        "active": bool(row.get("active", True)),
        "created_at": row.get("created_at", ""),
        "updated_at": row.get("updated_at", ""),
    }

def _calc_attendance_days_for_month(records: list[dict]) -> dict[str, float]:
    grouped: dict[str, set[str]] = {}
    for r in records:
        if r.get("status") != "present":
            continue
        emp_id = str(r.get("employee_id", ""))
        ts = str(r.get("ts", ""))
        if len(ts) < 10:
            continue
        day = ts[:10]
        s = grouped.get(emp_id)
        if s is None:
            s = set()
            grouped[emp_id] = s
        s.add(day)
    return {k: float(len(v)) for k, v in grouped.items()}


@router.get("/admin/employees")
def admin_list_employees(
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
    q: str | None = Query(default=None),
):
    rows = db.read_all("employees")
    if q:
        q2 = q.strip().lower()
        rows = [
            r
            for r in rows
            if q2 in str(r.get("employee_id", "")).lower() or q2 in str(r.get("name", "")).lower()
        ]
    month = datetime.now().strftime("%Y-%m")
    attendance_records = db.find_many("attendance_records", lambda r: str(r.get("ts", "")).startswith(month))
    days_map = _calc_attendance_days_for_month(attendance_records)
    for r in rows:
        emp_id = str(r.get("employee_id", ""))
        computed = days_map.get(emp_id)
        if computed is None:
            continue
        r["attendance_days"] = computed
        def updater(row: dict) -> dict:
            row["attendance_days"] = computed
            row["updated_at"] = now_iso()
            return row
        db.update_one("employees", lambda e: e.get("employee_id") == emp_id, updater)
    rows.sort(key=lambda r: str(r.get("employee_id", "")))
    return {"items": [_employee_public(r) for r in rows]}


@router.post("/admin/employees", status_code=status.HTTP_201_CREATED)
def admin_create_employee(
    payload: EmployeeCreate,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    exists = db.find_one("employees", lambda e: e.get("employee_id") == payload.employee_id)
    if exists:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="工号已存在")

    row = {
        "employee_id": payload.employee_id,
        "name": payload.name,
        "department": payload.department,
        "position": payload.position,
        "work_type": payload.work_type if payload.work_type in {"onsite", "offsite"} else "onsite",
        "daily_salary": payload.daily_salary,
        "attendance_days": payload.attendance_days,
        "active": True,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    db.insert("employees", row)
    db.insert(
        "employee_change_history",
        {
            "id": str(uuid.uuid4()),
            "employee_id": payload.employee_id,
            "changed_by": admin.user_id,
            "changed_at": now_iso(),
            "action": "create",
            "before": None,
            "after": row,
        },
    )
    return _employee_public(row)


@router.get("/admin/employees/{employee_id}")
def admin_get_employee_detail(
    employee_id: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == employee_id)
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")
    salaries = db.find_many("salary_records", lambda r: r.get("employee_id") == employee_id)
    overtime = db.find_many("overtime_requests", lambda r: r.get("employee_id") == employee_id)
    attendance = db.find_many("attendance_records", lambda r: r.get("employee_id") == employee_id)
    history = db.find_many("employee_change_history", lambda r: r.get("employee_id") == employee_id)
    history.sort(key=lambda r: r.get("changed_at", ""))
    return {
        "employee": _employee_public(emp),
        "salary_records": salaries,
        "overtime_records": overtime,
        "attendance_records": attendance,
        "change_history": history,
    }


@router.put("/admin/employees/{employee_id}")
def admin_update_employee(
    employee_id: str,
    payload: EmployeeUpdate,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    before = db.find_one("employees", lambda e: e.get("employee_id") == employee_id)
    if not before:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")

    update_data = payload.model_dump(exclude_unset=True)
    if "work_type" in update_data and update_data["work_type"] is not None and update_data["work_type"] not in {"onsite", "offsite"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="人员类型错误")
    if not update_data:
        return _employee_public(before)

    def updater(row: dict) -> dict:
        for k, v in update_data.items():
            row[k] = v
        row["updated_at"] = now_iso()
        return row

    after = db.update_one("employees", lambda e: e.get("employee_id") == employee_id, updater)
    if not after:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")

    db.insert(
        "employee_change_history",
        {
            "id": str(uuid.uuid4()),
            "employee_id": employee_id,
            "changed_by": admin.user_id,
            "changed_at": now_iso(),
            "action": "update",
            "before": before,
            "after": after,
        },
    )
    return _employee_public(after)


@router.delete("/admin/employees/{employee_id}")
def admin_delete_employee(
    employee_id: str,
    confirm: bool = Query(default=False),
    admin: Annotated[AuthUser, Depends(require_admin)] = None,
    db: Annotated[SQLiteDB, Depends(get_db)] = None,
):
    if not confirm:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="需要确认删除")
    before = db.find_one("employees", lambda e: e.get("employee_id") == employee_id)
    if not before:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")
    ok = db.delete_one("employees", lambda e: e.get("employee_id") == employee_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")
    db.insert(
        "employee_change_history",
        {
            "id": str(uuid.uuid4()),
            "employee_id": employee_id,
            "changed_by": admin.user_id,
            "changed_at": now_iso(),
            "action": "delete",
            "before": before,
            "after": None,
        },
    )
    return {"ok": True}


@router.get("/me/employee")
def employee_me(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id)
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工信息不存在")
    month = datetime.now().strftime("%Y-%m")
    attendance_records = db.find_many(
        "attendance_records",
        lambda r: r.get("employee_id") == user.user_id and str(r.get("ts", "")).startswith(month),
    )
    days_map = _calc_attendance_days_for_month(attendance_records)
    computed = days_map.get(user.user_id)
    if computed is not None:
        emp["attendance_days"] = computed
        def updater(row: dict) -> dict:
            row["attendance_days"] = computed
            row["updated_at"] = now_iso()
            return row
        db.update_one("employees", lambda e: e.get("employee_id") == user.user_id, updater)
    return _employee_public(emp)


class ChangeRequestCreate(BaseModel):
    field: str = Field(min_length=1)
    new_value: str = Field(min_length=1)
    reason: str = Field(min_length=1)


@router.post("/me/change-requests", status_code=status.HTTP_201_CREATED)
def employee_create_change_request(
    payload: ChangeRequestCreate,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id and e.get("active", True))
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工信息不存在")
    req = {
        "id": str(uuid.uuid4()),
        "employee_id": user.user_id,
        "field": payload.field,
        "new_value": payload.new_value,
        "reason": payload.reason,
        "status": "pending",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "approved_by": "",
        "approved_at": "",
        "rejected_reason": "",
    }
    db.insert("employee_change_requests", req)
    return req


@router.get("/me/change-requests")
def employee_list_change_requests(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    items = db.find_many("employee_change_requests", lambda r: r.get("employee_id") == user.user_id)
    items.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return {"items": items}


@router.get("/admin/change-requests")
def admin_list_change_requests(
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
    status_filter: str | None = Query(default=None, alias="status"),
):
    items = db.read_all("employee_change_requests")
    if status_filter:
        items = [r for r in items if r.get("status") == status_filter]
    items.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return {"items": items}


class RejectIn(BaseModel):
    reason: str = Field(min_length=1)


@router.post("/admin/change-requests/{request_id}/approve")
def admin_approve_change_request(
    request_id: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    req = db.find_one("employee_change_requests", lambda r: r.get("id") == request_id)
    if not req:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="申请不存在")
    if req.get("status") != "pending":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="申请已处理")

    emp_id = req.get("employee_id")
    emp_before = db.find_one("employees", lambda e: e.get("employee_id") == emp_id)
    if not emp_before:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")

    field_name = str(req.get("field", "")).strip()
    if field_name == "employee_id":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不允许修改工号")
    if field_name == "work_type":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不允许通过申请修改人员类型")

    new_value_raw = req.get("new_value", "")
    cast_value: Any = new_value_raw
    if field_name in {"daily_salary", "attendance_days"}:
        try:
            cast_value = float(new_value_raw)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="字段类型错误")
    if field_name in {"active"}:
        cast_value = str(new_value_raw).lower() in {"1", "true", "yes", "y"}

    def emp_updater(row: dict) -> dict:
        row[field_name] = cast_value
        row["updated_at"] = now_iso()
        return row

    emp_after = db.update_one("employees", lambda e: e.get("employee_id") == emp_id, emp_updater)
    if not emp_after:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")

    def req_updater(row: dict) -> dict:
        row["status"] = "approved"
        row["approved_by"] = admin.user_id
        row["approved_at"] = now_iso()
        row["updated_at"] = now_iso()
        return row

    db.update_one("employee_change_requests", lambda r: r.get("id") == request_id, req_updater)
    db.insert(
        "employee_change_history",
        {
            "id": str(uuid.uuid4()),
            "employee_id": emp_id,
            "changed_by": admin.user_id,
            "changed_at": now_iso(),
            "action": "approve_request",
            "before": emp_before,
            "after": emp_after,
            "request_id": request_id,
        },
    )
    return {"ok": True}


@router.post("/admin/change-requests/{request_id}/reject")
def admin_reject_change_request(
    request_id: str,
    payload: RejectIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[SQLiteDB, Depends(get_db)],
):
    req = db.find_one("employee_change_requests", lambda r: r.get("id") == request_id)
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

    db.update_one("employee_change_requests", lambda r: r.get("id") == request_id, updater)
    return {"ok": True}
