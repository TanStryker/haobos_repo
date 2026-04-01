import csv
import io
from collections import defaultdict
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, get_db, require_admin, require_user
from hrms.storage.json_db import JsonDB, now_iso


router = APIRouter(tags=["salary"])


def _attendance_days(db: JsonDB, employee_id: str, month: str) -> float:
    records = db.find_many(
        "attendance_records",
        lambda r: r.get("employee_id") == employee_id and str(r.get("ts", "")).startswith(month) and r.get("status") == "present",
    )
    days = set()
    for r in records:
        ts = str(r.get("ts", ""))
        if len(ts) >= 10:
            days.add(ts[:10])
    return float(len(days))


def _overtime_days(db: JsonDB, employee_id: str, month: str) -> float:
    records = db.find_many(
        "overtime_requests",
        lambda r: r.get("employee_id") == employee_id and r.get("status") == "approved" and str(r.get("date", "")).startswith(month),
    )
    total = 0.0
    for r in records:
        total += float(r.get("days", 0) or 0)
    return total


def _calc_salary(db: JsonDB, employee: dict, month: str) -> dict:
    emp_id = str(employee.get("employee_id", ""))
    daily_salary = float(employee.get("daily_salary", 0) or 0)
    attendance_days = _attendance_days(db, emp_id, month)
    overtime_days = _overtime_days(db, emp_id, month)
    total_salary = (attendance_days + overtime_days) * daily_salary
    return {
        "employee_id": emp_id,
        "month": month,
        "attendance_days": attendance_days,
        "overtime_days": overtime_days,
        "daily_salary": daily_salary,
        "total_salary": total_salary,
        "calculated_at": now_iso(),
    }


def _upsert_salary(db: JsonDB, record: dict) -> dict:
    existing = db.find_one("salary_records", lambda r: r.get("employee_id") == record["employee_id"] and r.get("month") == record["month"])
    if not existing:
        db.insert("salary_records", record)
        return record

    def updater(row: dict) -> dict:
        row.update(record)
        row["updated_at"] = now_iso()
        return row

    updated = db.update_one(
        "salary_records",
        lambda r: r.get("employee_id") == record["employee_id"] and r.get("month") == record["month"],
        updater,
    )
    return updated or record


@router.get("/admin/salaries")
def admin_list_salaries(
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    items = db.find_many("salary_records", lambda r: r.get("month") == month)
    items.sort(key=lambda r: r.get("employee_id", ""))
    return {"month": month, "items": items}


@router.post("/admin/salaries/calculate")
def admin_calculate_salaries(
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
    employee_id: str | None = Query(default=None),
):
    employees = db.read_all("employees")
    employees = [e for e in employees if e.get("active", True)]
    if employee_id:
        employees = [e for e in employees if e.get("employee_id") == employee_id]
        if not employees:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")
    results = []
    for emp in employees:
        rec = _calc_salary(db, emp, month)
        results.append(_upsert_salary(db, rec))
    results.sort(key=lambda r: r.get("employee_id", ""))
    return {"month": month, "items": results}


class SalaryAdjustIn(BaseModel):
    daily_salary: float | None = Field(default=None, ge=0)
    attendance_days: float | None = Field(default=None, ge=0)


@router.post("/admin/salaries/{employee_id}/adjust")
def admin_adjust_salary_inputs(
    employee_id: str,
    payload: SalaryAdjustIn,
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == employee_id)
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工不存在")

    update_data = payload.model_dump(exclude_unset=True)
    if "daily_salary" in update_data:
        def updater(row: dict) -> dict:
            row["daily_salary"] = update_data["daily_salary"]
            row["updated_at"] = now_iso()
            return row
        db.update_one("employees", lambda e: e.get("employee_id") == employee_id, updater)
        emp = db.find_one("employees", lambda e: e.get("employee_id") == employee_id) or emp

    if "attendance_days" in update_data:
        db.insert(
            "attendance_manual_overrides",
            {
                "id": now_iso() + ":" + employee_id + ":" + month,
                "employee_id": employee_id,
                "month": month,
                "attendance_days": update_data["attendance_days"],
                "updated_by": admin.user_id,
                "updated_at": now_iso(),
            },
        )

    rec = _calc_salary(db, emp, month)
    if "attendance_days" in update_data:
        rec["attendance_days"] = update_data["attendance_days"]
        rec["total_salary"] = (rec["attendance_days"] + rec["overtime_days"]) * rec["daily_salary"]
    saved = _upsert_salary(db, rec)
    return {"month": month, "item": saved}


@router.get("/admin/salaries/export")
def admin_export_salaries(
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    items = db.find_many("salary_records", lambda r: r.get("month") == month)
    items.sort(key=lambda r: r.get("employee_id", ""))
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["employee_id", "month", "attendance_days", "overtime_days", "daily_salary", "total_salary"])
    for r in items:
        writer.writerow(
            [
                r.get("employee_id", ""),
                r.get("month", ""),
                r.get("attendance_days", 0),
                r.get("overtime_days", 0),
                r.get("daily_salary", 0),
                r.get("total_salary", 0),
            ]
        )
    data = buf.getvalue().encode("utf-8-sig")
    return Response(content=data, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=salaries_{month}.csv"})


@router.get("/me/salary")
def employee_salary(
    month: str,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    rec = db.find_one("salary_records", lambda r: r.get("employee_id") == user.user_id and r.get("month") == month)
    if not rec:
        emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id and e.get("active", True))
        if not emp:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工信息不存在")
        rec = _upsert_salary(db, _calc_salary(db, emp, month))
    return rec


@router.get("/me/salary/history")
def employee_salary_history(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[JsonDB, Depends(get_db)]):
    items = db.find_many("salary_records", lambda r: r.get("employee_id") == user.user_id)
    items.sort(key=lambda r: r.get("month", ""), reverse=True)
    return {"items": items}

