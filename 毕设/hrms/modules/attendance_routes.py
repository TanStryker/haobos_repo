import uuid
from collections import defaultdict
from datetime import datetime
import math
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from hrms.core.auth import AuthUser, get_db, require_admin, require_user
from hrms.storage.json_db import JsonDB, now_iso


router = APIRouter(tags=["attendance"])


class AttendanceRecordIn(BaseModel):
    employee_id: str = Field(min_length=1)
    ts: str = Field(min_length=10)
    status: str = Field(min_length=1, default="present")


class SyncIn(BaseModel):
    start_date: str = Field(min_length=10, max_length=10)
    end_date: str = Field(min_length=10, max_length=10)
    records: list[AttendanceRecordIn] | None = None


class AttendanceRuleIn(BaseModel):
    name: str = Field(min_length=1, default="默认规则")
    enabled: bool = True
    start_time: str = Field(min_length=4, max_length=5, default="09:00")
    end_time: str = Field(min_length=4, max_length=5, default="10:00")
    center_lat: float | None = None
    center_lng: float | None = None
    allowed_radius_m: int | None = Field(default=None, ge=0)
    address_hint: str | None = None


def _parse_hhmm(s: str) -> tuple[int, int]:
    parts = s.split(":")
    if len(parts) != 2:
        raise ValueError("时间格式错误")
    h = int(parts[0])
    m = int(parts[1])
    if h < 0 or h > 23 or m < 0 or m > 59:
        raise ValueError("时间格式错误")
    return h, m


def _minutes_of_day(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _get_current_rule(db: JsonDB) -> dict:
    rule = db.find_one("attendance_rules", lambda r: r.get("id") == "current")
    if rule:
        return rule
    return {
        "id": "current",
        "name": "默认规则",
        "enabled": False,
        "start_time": "09:00",
        "end_time": "10:00",
        "center_lat": None,
        "center_lng": None,
        "allowed_radius_m": None,
        "address_hint": "",
        "updated_at": "",
        "updated_by": "",
    }


def _validate_punch(rule: dict, punch_dt: datetime, lat: float | None, lng: float | None) -> tuple[bool, str]:
    if not rule.get("enabled", False):
        return False, "未启用打卡规则"
    try:
        sh, sm = _parse_hhmm(str(rule.get("start_time", "")))
        eh, em = _parse_hhmm(str(rule.get("end_time", "")))
    except Exception:
        return False, "打卡规则时间配置错误"
    start_min = sh * 60 + sm
    end_min = eh * 60 + em
    now_min = _minutes_of_day(punch_dt)
    if now_min < start_min or now_min > end_min:
        return False, "不在规定打卡时间范围内"

    center_lat = rule.get("center_lat")
    center_lng = rule.get("center_lng")
    radius_m = rule.get("allowed_radius_m")
    if center_lat is None or center_lng is None or radius_m is None:
        return True, ""
    if lat is None or lng is None:
        return False, "未获取到定位信息"
    dist = _haversine_m(float(lat), float(lng), float(center_lat), float(center_lng))
    if dist > float(radius_m):
        return False, "不在规定打卡地点范围内"
    return True, ""


@router.post("/admin/attendance/sync")
def admin_sync_attendance(
    payload: SyncIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    records = payload.records or []
    inserted = 0
    for r in records:
        row = {
            "id": str(uuid.uuid4()),
            "employee_id": r.employee_id,
            "ts": r.ts,
            "status": r.status,
            "source": "hikvision_placeholder",
            "created_at": now_iso(),
        }
        db.insert("attendance_records", row)
        inserted += 1
    return {"ok": True, "inserted": inserted}


@router.get("/admin/attendance/rule")
def admin_get_attendance_rule(admin: Annotated[AuthUser, Depends(require_admin)], db: Annotated[JsonDB, Depends(get_db)]):
    return _get_current_rule(db)


@router.post("/admin/attendance/rule")
def admin_set_attendance_rule(
    payload: AttendanceRuleIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    rule = payload.model_dump()
    row = {
        "id": "current",
        "name": rule["name"],
        "enabled": bool(rule["enabled"]),
        "start_time": rule["start_time"],
        "end_time": rule["end_time"],
        "center_lat": rule.get("center_lat"),
        "center_lng": rule.get("center_lng"),
        "allowed_radius_m": rule.get("allowed_radius_m"),
        "address_hint": rule.get("address_hint") or "",
        "updated_at": now_iso(),
        "updated_by": admin.user_id,
    }

    existing = db.find_one("attendance_rules", lambda r: r.get("id") == "current")
    if not existing:
        db.insert("attendance_rules", row)
        return {"ok": True}

    def updater(r: dict) -> dict:
        r.update(row)
        return r

    db.update_one("attendance_rules", lambda r: r.get("id") == "current", updater)
    return {"ok": True}


@router.get("/attendance/rule")
def employee_get_attendance_rule(user: Annotated[AuthUser, Depends(require_user)], db: Annotated[JsonDB, Depends(get_db)]):
    rule = _get_current_rule(db)
    public = dict(rule)
    public.pop("updated_by", None)
    return public


class PunchIn(BaseModel):
    ts: str | None = None
    address: str = Field(min_length=1)
    lat: float | None = None
    lng: float | None = None


@router.post("/me/attendance/punch", status_code=status.HTTP_201_CREATED)
def employee_punch(
    payload: PunchIn,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    emp = db.find_one("employees", lambda e: e.get("employee_id") == user.user_id and e.get("active", True))
    if not emp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="员工信息不存在")
    if payload.ts:
        try:
            punch_dt = datetime.fromisoformat(payload.ts)
        except Exception:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="打卡时间格式错误")
    else:
        punch_dt = datetime.now()
    ts_str = punch_dt.strftime("%Y-%m-%dT%H:%M:%S")

    rule = _get_current_rule(db)
    ok, reason = _validate_punch(rule, punch_dt, payload.lat, payload.lng)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"无效打卡：{reason}")

    row = {
        "id": str(uuid.uuid4()),
        "employee_id": user.user_id,
        "ts": ts_str,
        "status": "present",
        "source": "wechat_miniprogram",
        "address": payload.address,
        "lat": payload.lat,
        "lng": payload.lng,
        "rule_id": rule.get("id", "current"),
        "created_at": now_iso(),
    }
    db.insert("attendance_records", row)
    return {"ok": True, "item": row}


@router.get("/me/attendance/punches")
def employee_punch_history(
    limit: int = Query(default=5, ge=1, le=50),
    user: Annotated[AuthUser, Depends(require_user)] = None,
    db: Annotated[JsonDB, Depends(get_db)] = None,
):
    items = db.find_many(
        "attendance_records",
        lambda r: r.get("employee_id") == user.user_id and r.get("source") == "wechat_miniprogram",
    )
    items.sort(key=lambda r: str(r.get("ts", "")), reverse=True)
    return {"items": items[:limit]}


@router.get("/admin/attendance/records")
def admin_list_attendance_records(
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
    employee_id: str | None = Query(default=None),
    month: str | None = Query(default=None),
):
    items = db.read_all("attendance_records")
    if employee_id:
        items = [r for r in items if r.get("employee_id") == employee_id]
    if month:
        items = [r for r in items if str(r.get("ts", "")).startswith(month)]
    items.sort(key=lambda r: (r.get("employee_id", ""), r.get("ts", "")))
    return {"items": items}


class AdjustIn(BaseModel):
    employee_id: str = Field(min_length=1)
    ts: str = Field(min_length=10)
    status: str = Field(min_length=1)


@router.post("/admin/attendance/adjust")
def admin_adjust_attendance(
    payload: AdjustIn,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    existing = db.find_one("attendance_records", lambda r: r.get("employee_id") == payload.employee_id and r.get("ts") == payload.ts)
    if existing:
        def updater(row: dict) -> dict:
            row["status"] = payload.status
            row["updated_at"] = now_iso()
            row["adjusted_by"] = admin.user_id
            return row
        db.update_one("attendance_records", lambda r: r.get("id") == existing.get("id"), updater)
        return {"ok": True, "action": "updated"}
    row = {
        "id": str(uuid.uuid4()),
        "employee_id": payload.employee_id,
        "ts": payload.ts,
        "status": payload.status,
        "source": "manual_adjust",
        "created_at": now_iso(),
        "adjusted_by": admin.user_id,
    }
    db.insert("attendance_records", row)
    return {"ok": True, "action": "inserted"}


def _attendance_days(records: list[dict]) -> float:
    days = set()
    for r in records:
        if r.get("status") != "present":
            continue
        ts = str(r.get("ts", ""))
        if len(ts) >= 10:
            days.add(ts[:10])
    return float(len(days))


@router.get("/admin/attendance/stats")
def admin_attendance_stats(
    month: str,
    admin: Annotated[AuthUser, Depends(require_admin)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    items = db.find_many("attendance_records", lambda r: str(r.get("ts", "")).startswith(month))
    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in items:
        grouped[str(r.get("employee_id", ""))].append(r)
    out = [{"employee_id": emp_id, "attendance_days": _attendance_days(recs)} for emp_id, recs in grouped.items()]
    out.sort(key=lambda x: x["employee_id"])
    return {"month": month, "items": out}


@router.get("/me/attendance/stats")
def employee_attendance_stats(
    month: str,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    items = db.find_many("attendance_records", lambda r: r.get("employee_id") == user.user_id and str(r.get("ts", "")).startswith(month))
    return {"month": month, "employee_id": user.user_id, "attendance_days": _attendance_days(items)}


@router.get("/me/attendance/records")
def employee_attendance_records(
    month: str,
    user: Annotated[AuthUser, Depends(require_user)],
    db: Annotated[JsonDB, Depends(get_db)],
):
    items = db.find_many("attendance_records", lambda r: r.get("employee_id") == user.user_id and str(r.get("ts", "")).startswith(month))
    items.sort(key=lambda r: r.get("ts", ""))
    return {"items": items}
