from typing import Annotated

from fastapi import APIRouter, Depends

from hrms.core.auth import AuthUser, get_db, require_user
from hrms.storage.json_db import JsonDB


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _workflow_step(status: str) -> str:
    if status == "pending":
        return "待管理员审批"
    if status == "approved":
        return "已通过"
    if status == "rejected":
        return "已驳回"
    return status or "未知"


@router.get("/summary")
def get_dashboard_summary(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[JsonDB, Depends(get_db)]):
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

