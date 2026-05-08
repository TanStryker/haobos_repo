from typing import Annotated

from fastapi import APIRouter, Depends

from hrms.core.auth import AuthUser, get_db, require_user
from hrms.storage.sqlite_db import SQLiteDB


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _workflow_step(status: str) -> str:
    if status == "pending":
        return "待管理员审批"
    if status == "approved":
        return "已通过"
    if status == "rejected":
        return "已驳回"
    return status or "未知"


@router.get("/overview")
def get_dashboard_overview(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    employees = db.read_all("employees")
    users = db.read_all("users")
    rules = db.read_all("attendance_rules")
    records = db.read_all("attendance_records")
    change_requests = db.read_all("employee_change_requests")
    overtime_requests = db.read_all("overtime_requests")

    if user.role == "admin":
        active_employees = [e for e in employees if bool(e.get("active", True))]
        pending_change = [r for r in change_requests if r.get("status") == "pending"]
        pending_overtime = [r for r in overtime_requests if r.get("status") == "pending"]
        enabled_rules = [r for r in rules if bool(r.get("enabled"))]
        return {
            "role": "admin",
            "counts": {
                "employees_total": len(employees),
                "employees_active": len(active_employees),
                "users_total": len(users),
                "attendance_rules_enabled": len(enabled_rules),
                "pending_change_requests": len(pending_change),
                "pending_overtime_requests": len(pending_overtime),
                "attendance_records_total": len(records),
            },
        }

    my_changes = [r for r in change_requests if r.get("employee_id") == user.user_id]
    my_overtime = [r for r in overtime_requests if r.get("employee_id") == user.user_id]
    my_pending_change = [r for r in my_changes if r.get("status") == "pending"]
    my_pending_overtime = [r for r in my_overtime if r.get("status") == "pending"]
    my_emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id)
    my_records = [r for r in records if r.get("employee_id") == user.user_id]
    my_last_check = ""
    if my_records:
        my_records.sort(key=lambda x: str(x.get("ts", "")), reverse=True)
        my_last_check = str(my_records[0].get("ts", "")) or ""

    return {
        "role": "employee",
        "profile": {
            "employee_id": user.user_id,
            "name": (my_emp or {}).get("name", ""),
            "department": (my_emp or {}).get("department", ""),
            "position": (my_emp or {}).get("position", ""),
            "work_type": (my_emp or {}).get("work_type", ""),
        },
        "counts": {
            "pending_change_requests": len(my_pending_change),
            "pending_overtime_requests": len(my_pending_overtime),
            "attendance_records_total": len(my_records),
        },
        "last_check_time": my_last_check,
    }


@router.get("/summary")
def get_dashboard_summary(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[SQLiteDB, Depends(get_db)]):
    if user.role == "admin":
        pending_change = db.find_many("employee_change_requests", lambda r: r.get("status") == "pending")
        pending_overtime = db.find_many("overtime_requests", lambda r: r.get("status") == "pending")
        items = [
            {"module": "员工信息管理", "flow": "信息修改申请", "pending_count": len(pending_change), "link": "/ui/admin_change_requests.html"},
            {"module": "加班审批", "flow": "加班申请", "pending_count": len(pending_overtime), "link": "/ui/admin_overtime.html"},
        ]
        total_pending = sum(i["pending_count"] for i in items)
        return {"role": "admin", "total_pending": total_pending, "pending_by_module": items}

    change_rows = db.find_many("employee_change_requests", lambda r: r.get("employee_id") == user.user_id)
    overtime_rows = db.find_many("overtime_requests", lambda r: r.get("employee_id") == user.user_id)

    pending = []
    for r in change_rows:
        if r.get("status") == "pending":
            pending.append(
                {
                    "type": "信息修改申请",
                    "module": "员工信息管理",
                    "created_at": r.get("created_at", ""),
                    "status": r.get("status", ""),
                    "step": _workflow_step(r.get("status", "")),
                    "summary": f"{r.get('field', '')} -> {r.get('new_value', '')}",
                    "link": "/ui/employee_change_requests.html",
                }
            )
    for r in overtime_rows:
        if r.get("status") == "pending":
            pending.append(
                {
                    "type": "加班申请",
                    "module": "加班审批",
                    "created_at": r.get("created_at", ""),
                    "status": r.get("status", ""),
                    "step": _workflow_step(r.get("status", "")),
                    "summary": f"{r.get('date', '')} {r.get('days', '')}天",
                    "link": "/ui/employee_overtime.html",
                }
            )

    def _created_at_key(x: dict) -> str:
        return str(x.get("created_at", ""))

    history = []
    for r in change_rows:
        history.append(
            {
                "type": "信息修改申请",
                "module": "员工信息管理",
                "created_at": r.get("created_at", ""),
                "status": r.get("status", ""),
                "step": _workflow_step(r.get("status", "")),
                "summary": f"{r.get('field', '')} -> {r.get('new_value', '')}",
                "result": r.get("rejected_reason", "") if r.get("status") == "rejected" else "",
                "link": "/ui/employee_change_requests.html",
            }
        )
    for r in overtime_rows:
        history.append(
            {
                "type": "加班申请",
                "module": "加班审批",
                "created_at": r.get("created_at", ""),
                "status": r.get("status", ""),
                "step": _workflow_step(r.get("status", "")),
                "summary": f"{r.get('date', '')} {r.get('days', '')}天",
                "result": r.get("rejected_reason", "") if r.get("status") == "rejected" else "",
                "link": "/ui/employee_overtime.html",
            }
        )

    history.sort(key=_created_at_key, reverse=True)
    pending.sort(key=_created_at_key, reverse=True)
    return {"role": "employee", "pending": pending, "history_last5": history[:5]}

