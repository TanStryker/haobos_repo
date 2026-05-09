import json
import os
import sqlite3
import threading
import uuid
from datetime import date, datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def init_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS employee (
            emp_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            gender TEXT,
            age INTEGER,
            department TEXT NOT NULL,
            position TEXT NOT NULL,
            phone TEXT,
            hire_date TEXT NOT NULL,
            daily_salary NUMERIC NOT NULL DEFAULT 0.00 CHECK (daily_salary >= 0),
            attendance_days REAL NOT NULL DEFAULT 0 CHECK (attendance_days >= 0 AND attendance_days <= 31),
            work_type TEXT NOT NULL DEFAULT 'onsite' CHECK (work_type IN ('onsite','offsite')),
            active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
            create_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (age IS NULL OR (age >= 18 AND age <= 60))
        );

        CREATE TRIGGER IF NOT EXISTS employee_update_time
        AFTER UPDATE ON employee
        FOR EACH ROW
        BEGIN
            UPDATE employee SET update_time = CURRENT_TIMESTAMP WHERE emp_id = OLD.emp_id;
        END;

        CREATE TABLE IF NOT EXISTS user (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            emp_id TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee' CHECK (role IN ('admin','employee')),
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive')),
            must_change_password INTEGER NOT NULL DEFAULT 0 CHECK (must_change_password IN (0,1)),
            create_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (emp_id) REFERENCES employee(emp_id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE TRIGGER IF NOT EXISTS user_update_time
        AFTER UPDATE ON user
        FOR EACH ROW
        BEGIN
            UPDATE user SET update_time = CURRENT_TIMESTAMP WHERE user_id = OLD.user_id;
        END;

        CREATE TABLE IF NOT EXISTS overtime (
            overtime_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ext_id TEXT NOT NULL UNIQUE,
            emp_id TEXT NOT NULL,
            start_date TEXT NOT NULL DEFAULT '',
            end_date TEXT NOT NULL DEFAULT '',
            days REAL NOT NULL DEFAULT 0.0 CHECK (days >= 0),
            reason TEXT NOT NULL,
            apply_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
            approver_id INTEGER,
            approve_time TEXT,
            reject_reason TEXT,
            update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (emp_id) REFERENCES employee(emp_id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (approver_id) REFERENCES user(user_id) ON UPDATE CASCADE ON DELETE SET NULL
        );

        CREATE TRIGGER IF NOT EXISTS overtime_update_time
        AFTER UPDATE ON overtime
        FOR EACH ROW
        BEGIN
            UPDATE overtime SET update_time = CURRENT_TIMESTAMP WHERE overtime_id = OLD.overtime_id;
        END;

        CREATE TABLE IF NOT EXISTS check_record (
            check_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ext_id TEXT NOT NULL UNIQUE,
            emp_id TEXT NOT NULL,
            check_time TEXT NOT NULL,
            check_status TEXT NOT NULL DEFAULT 'normal',
            check_location TEXT,
            source TEXT,
            address TEXT,
            lat REAL,
            lng REAL,
            rule_id TEXT,
            create_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (emp_id) REFERENCES employee(emp_id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_check_record_emp_time ON check_record(emp_id, check_time);

        CREATE TABLE IF NOT EXISTS info_modify_apply (
            modify_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ext_id TEXT NOT NULL UNIQUE,
            emp_id TEXT NOT NULL,
            modify_field TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT NOT NULL,
            reason TEXT NOT NULL,
            apply_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
            approver_id INTEGER,
            approve_time TEXT,
            reject_reason TEXT,
            update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (emp_id) REFERENCES employee(emp_id) ON UPDATE CASCADE ON DELETE RESTRICT,
            FOREIGN KEY (approver_id) REFERENCES user(user_id) ON UPDATE CASCADE ON DELETE SET NULL
        );

        CREATE TRIGGER IF NOT EXISTS info_modify_apply_update_time
        AFTER UPDATE ON info_modify_apply
        FOR EACH ROW
        BEGIN
            UPDATE info_modify_apply SET update_time = CURRENT_TIMESTAMP WHERE modify_id = OLD.modify_id;
        END;

        CREATE TABLE IF NOT EXISTS system_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            operation_type TEXT NOT NULL,
            operation_content TEXT NOT NULL,
            operation_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            operation_ip TEXT,
            operation_result TEXT NOT NULL DEFAULT 'success' CHECK (operation_result IN ('success','failed')),
            FOREIGN KEY (user_id) REFERENCES user(user_id) ON UPDATE CASCADE ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_system_log_user_time ON system_log(user_id, operation_time);

        CREATE TABLE IF NOT EXISTS salary_record (
            id TEXT PRIMARY KEY,
            employee_id TEXT NOT NULL,
            month TEXT NOT NULL,
            attendance_days REAL NOT NULL DEFAULT 0,
            overtime_days REAL NOT NULL DEFAULT 0,
            daily_salary NUMERIC NOT NULL DEFAULT 0.00,
            total_salary NUMERIC NOT NULL DEFAULT 0.00,
            calculated_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(employee_id, month)
        );

        CREATE TABLE IF NOT EXISTS attendance_manual_override (
            id TEXT PRIMARY KEY,
            employee_id TEXT NOT NULL,
            month TEXT NOT NULL,
            attendance_days REAL NOT NULL DEFAULT 0,
            updated_by TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS attendance_rule (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
            work_type TEXT NOT NULL DEFAULT 'onsite' CHECK (work_type IN ('onsite','offsite')),
            priority INTEGER NOT NULL DEFAULT 100,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            center_lat REAL,
            center_lng REAL,
            allowed_radius_m INTEGER,
            address_hint TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS employee_change_history (
            id TEXT PRIMARY KEY,
            employee_id TEXT NOT NULL,
            changed_by TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            action TEXT NOT NULL,
            before_json TEXT,
            after_json TEXT,
            request_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_employee_change_history_emp_time ON employee_change_history(employee_id, changed_at);

        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT NOT NULL DEFAULT ''
        );

        DROP TABLE IF EXISTS session_store;
        CREATE TABLE session_store (
            user_id TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            role TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            tab_id TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_session_store_token ON session_store(token);
        """
        with self._lock:
            self._conn.executescript(ddl)
            cols = {str(r["name"]) for r in self._conn.execute("PRAGMA table_info(overtime)").fetchall()}
            if "start_date" not in cols:
                self._conn.execute("ALTER TABLE overtime ADD COLUMN start_date TEXT NOT NULL DEFAULT ''")
            if "end_date" not in cols:
                self._conn.execute("ALTER TABLE overtime ADD COLUMN end_date TEXT NOT NULL DEFAULT ''")

    def upsert_session(self, user_id: str, token: str, role: str, expires_at: str, tab_id: str) -> None:
        uid = str(user_id or "").strip()
        tok = str(token or "").strip()
        r = str(role or "").strip() or "employee"
        exp = str(expires_at or "").strip()
        tab = str(tab_id or "").strip()
        if not uid or not tok or not exp or not tab:
            raise ValueError("invalid session params")
        with self._lock:
            self._conn.execute(
                "INSERT INTO session_store(user_id, token, role, expires_at, tab_id) VALUES(?,?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET token=excluded.token, role=excluded.role, expires_at=excluded.expires_at, tab_id=excluded.tab_id",
                (uid, tok, r, exp, tab),
            )

    def get_session_by_token(self, token: str) -> dict | None:
        tok = str(token or "").strip()
        if not tok:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT user_id, token, role, expires_at, tab_id FROM session_store WHERE token = ?",
                (tok,),
            ).fetchone()
            if not row:
                return None
            return dict(row)

    def delete_session_by_token(self, token: str) -> None:
        tok = str(token or "").strip()
        if not tok:
            return
        with self._lock:
            self._conn.execute("DELETE FROM session_store WHERE token = ?", (tok,))

    def read_all(self, name: str) -> list[dict]:
        if name == "employees":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM employee").fetchall()
            return [self._employee_to_public(r) for r in rows]
        if name == "users":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM user").fetchall()
            return [self._user_to_public(r) for r in rows]
        if name == "overtime_requests":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM overtime").fetchall()
            return [self._overtime_to_public(r) for r in rows]
        if name == "attendance_records":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM check_record").fetchall()
            return [self._check_record_to_public(r) for r in rows]
        if name == "employee_change_requests":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM info_modify_apply").fetchall()
            return [self._modify_apply_to_public(r) for r in rows]
        if name == "salary_records":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM salary_record").fetchall()
            return [dict(r) for r in rows]
        if name == "attendance_manual_overrides":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM attendance_manual_override").fetchall()
            return [dict(r) for r in rows]
        if name == "attendance_rules":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM attendance_rule").fetchall()
            return [self._attendance_rule_to_public(r) for r in rows]
        if name == "employee_change_history":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM employee_change_history").fetchall()
            return [self._history_to_public(r) for r in rows]
        if name == "system_config":
            with self._lock:
                rows = self._conn.execute("SELECT * FROM system_config").fetchall()
            return [dict(r) for r in rows]
        if name == "system_log":
            with self._lock:
                rows = self._conn.execute(
                    "SELECT l.log_id, u.emp_id AS user_id, l.operation_type, l.operation_content, l.operation_time, l.operation_ip, l.operation_result "
                    "FROM system_log l JOIN user u ON u.user_id = l.user_id"
                ).fetchall()
            return [dict(r) for r in rows]
        return []

    def write_all(self, name: str, rows: list[dict]) -> None:
        raise NotImplementedError("SQLiteDB 不支持 write_all，请使用 insert/update_one/delete_one")

    def insert(self, name: str, row: dict) -> dict:
        if name == "employees":
            return self._insert_employee(row)
        if name == "users":
            return self._insert_user(row)
        if name == "overtime_requests":
            return self._insert_overtime(row)
        if name == "attendance_records":
            return self._insert_check_record(row)
        if name == "employee_change_requests":
            return self._insert_modify_apply(row)
        if name == "salary_records":
            return self._insert_salary_record(row)
        if name == "attendance_manual_overrides":
            return self._insert_attendance_manual_override(row)
        if name == "attendance_rules":
            return self._insert_attendance_rule(row)
        if name == "employee_change_history":
            return self._insert_history(row)
        if name == "system_config":
            return self._insert_system_config(row)
        if name == "system_log":
            return self._insert_system_log(row)
        raise KeyError(f"unknown table: {name}")

    def find_one(self, name: str, predicate) -> dict | None:
        for row in self.read_all(name):
            if predicate(row):
                return row
        return None

    def find_many(self, name: str, predicate) -> list[dict]:
        return [row for row in self.read_all(name) if predicate(row)]

    def update_one(self, name: str, predicate, updater) -> dict | None:
        rows = self.read_all(name)
        changed = None
        for row in rows:
            if predicate(row):
                new_row = updater(dict(row))
                changed = new_row
                break
        if changed is None:
            return None
        if name == "employees":
            self._update_employee(changed)
            return changed
        if name == "users":
            self._update_user(changed)
            return changed
        if name == "overtime_requests":
            self._update_overtime(changed)
            return changed
        if name == "attendance_records":
            self._update_check_record(changed)
            return changed
        if name == "employee_change_requests":
            self._update_modify_apply(changed)
            return changed
        if name == "salary_records":
            self._update_salary_record(changed)
            return changed
        if name == "attendance_manual_overrides":
            self._update_attendance_manual_override(changed)
            return changed
        if name == "attendance_rules":
            self._update_attendance_rule(changed)
            return changed
        if name == "employee_change_history":
            self._update_history(changed)
            return changed
        if name == "system_config":
            self._update_system_config(changed)
            return changed
        raise KeyError(f"unknown table: {name}")

    def delete_one(self, name: str, predicate) -> bool:
        rows = self.read_all(name)
        target = None
        for row in rows:
            if predicate(row):
                target = row
                break
        if target is None:
            return False
        if name == "employees":
            with self._lock:
                cur = self._conn.execute("DELETE FROM employee WHERE emp_id = ?", (str(target.get("employee_id", "")),))
                return cur.rowcount > 0
        if name == "users":
            with self._lock:
                cur = self._conn.execute("DELETE FROM user WHERE emp_id = ?", (str(target.get("user_id", "")),))
                return cur.rowcount > 0
        if name == "overtime_requests":
            with self._lock:
                cur = self._conn.execute("DELETE FROM overtime WHERE ext_id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "attendance_records":
            with self._lock:
                cur = self._conn.execute("DELETE FROM check_record WHERE ext_id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "employee_change_requests":
            with self._lock:
                cur = self._conn.execute("DELETE FROM info_modify_apply WHERE ext_id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "employee_change_history":
            with self._lock:
                cur = self._conn.execute("DELETE FROM employee_change_history WHERE id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "attendance_rules":
            with self._lock:
                cur = self._conn.execute("DELETE FROM attendance_rule WHERE id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "salary_records":
            with self._lock:
                if target.get("id"):
                    cur = self._conn.execute("DELETE FROM salary_record WHERE id = ?", (str(target.get("id", "")),))
                else:
                    cur = self._conn.execute(
                        "DELETE FROM salary_record WHERE employee_id = ? AND month = ?",
                        (str(target.get("employee_id", "")), str(target.get("month", ""))),
                    )
                return cur.rowcount > 0
        if name == "attendance_manual_overrides":
            with self._lock:
                cur = self._conn.execute("DELETE FROM attendance_manual_override WHERE id = ?", (str(target.get("id", "")),))
                return cur.rowcount > 0
        if name == "system_config":
            with self._lock:
                cur = self._conn.execute("DELETE FROM system_config WHERE key = ?", (str(target.get("key", "")),))
                return cur.rowcount > 0
        raise KeyError(f"unknown table: {name}")

    def migrate_from_json_dir(self, data_dir: str) -> None:
        def _load(path: str) -> list[dict]:
            if not os.path.exists(path):
                return []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    v = json.load(f)
                return v if isinstance(v, list) else []
            except Exception:
                return []

        with self._lock:
            existing = self._conn.execute("SELECT COUNT(*) AS c FROM employee").fetchone()["c"]
        if existing:
            return

        employees = _load(os.path.join(data_dir, "employees.json"))
        users = _load(os.path.join(data_dir, "users.json"))
        overtime = _load(os.path.join(data_dir, "overtime_requests.json"))
        attendance = _load(os.path.join(data_dir, "attendance_records.json"))
        change_requests = _load(os.path.join(data_dir, "employee_change_requests.json"))
        change_history = _load(os.path.join(data_dir, "employee_change_history.json"))
        attendance_rules = _load(os.path.join(data_dir, "attendance_rules.json"))
        system_config = _load(os.path.join(data_dir, "system_config.json"))
        salary_records = _load(os.path.join(data_dir, "salary_records.json"))

        for e in employees:
            try:
                self.insert("employees", e)
            except Exception:
                continue

        for u in users:
            emp_id = str(u.get("user_id", "")).strip()
            if not emp_id:
                continue
            if not self.find_one("employees", lambda e: e.get("employee_id") == emp_id):
                try:
                    self.insert(
                        "employees",
                        {
                            "employee_id": emp_id,
                            "name": emp_id,
                            "department": "未设置",
                            "position": "未设置",
                            "work_type": "onsite",
                            "daily_salary": 0,
                            "attendance_days": 0,
                            "active": True,
                            "hire_date": "1970-01-01",
                        },
                    )
                except Exception:
                    pass
            try:
                self.insert("users", u)
            except Exception:
                continue

        for r in overtime:
            try:
                self.insert("overtime_requests", r)
            except Exception:
                continue

        for r in attendance:
            try:
                self.insert("attendance_records", r)
            except Exception:
                continue

        for r in change_requests:
            try:
                self.insert("employee_change_requests", r)
            except Exception:
                continue

        for r in change_history:
            try:
                self.insert("employee_change_history", r)
            except Exception:
                continue

        for r in attendance_rules:
            try:
                self.insert("attendance_rules", r)
            except Exception:
                continue

        for r in system_config:
            try:
                self.insert("system_config", r)
            except Exception:
                continue

        for r in salary_records:
            try:
                self.insert("salary_records", r)
            except Exception:
                continue

    def _employee_to_public(self, r: sqlite3.Row) -> dict:
        return {
            "employee_id": r["emp_id"],
            "name": r["name"],
            "gender": r["gender"],
            "age": r["age"],
            "department": r["department"],
            "position": r["position"],
            "phone": r["phone"],
            "hire_date": r["hire_date"],
            "daily_salary": float(r["daily_salary"] or 0),
            "attendance_days": float(r["attendance_days"] or 0),
            "work_type": r["work_type"],
            "active": bool(r["active"]),
            "created_at": r["create_time"],
            "updated_at": r["update_time"],
        }

    def _user_to_public(self, r: sqlite3.Row) -> dict:
        return {
            "user_id": r["emp_id"],
            "role": r["role"],
            "password_hash": r["password_hash"],
            "active": str(r["status"]) == "active",
            "status": r["status"],
            "created_at": r["create_time"],
            "updated_at": r["update_time"],
            "must_change_password": bool(r["must_change_password"]),
        }

    def _overtime_to_public(self, r: sqlite3.Row) -> dict:
        approved_by = ""
        if r["approver_id"] is not None:
            with self._lock:
                u = self._conn.execute("SELECT emp_id FROM user WHERE user_id = ?", (int(r["approver_id"]),)).fetchone()
            if u:
                approved_by = str(u["emp_id"])
        start_date = str(r["start_date"] or "")
        end_date = str(r["end_date"] or "")
        date_display = str(r["apply_time"])[:10]
        if start_date and end_date:
            date_display = start_date if start_date == end_date else f"{start_date}~{end_date}"
        return {
            "id": r["ext_id"],
            "employee_id": r["emp_id"],
            "date": date_display,
            "start_date": start_date,
            "end_date": end_date,
            "days": float(r["days"] or 0),
            "reason": r["reason"],
            "status": r["status"],
            "created_at": r["apply_time"],
            "updated_at": r["update_time"],
            "approved_by": approved_by,
            "approved_at": r["approve_time"] or "",
            "rejected_reason": r["reject_reason"] or "",
        }

    def _check_record_to_public(self, r: sqlite3.Row) -> dict:
        status_map = {"normal": "present"}
        status = str(r["check_status"])
        return {
            "id": r["ext_id"],
            "employee_id": r["emp_id"],
            "ts": r["check_time"],
            "status": status_map.get(status, status),
            "source": r["source"] or "",
            "address": r["address"] or "",
            "lat": r["lat"],
            "lng": r["lng"],
            "rule_id": r["rule_id"] or "",
            "created_at": r["create_time"],
        }

    def _modify_apply_to_public(self, r: sqlite3.Row) -> dict:
        approved_by = ""
        if r["approver_id"] is not None:
            with self._lock:
                u = self._conn.execute("SELECT emp_id FROM user WHERE user_id = ?", (int(r["approver_id"]),)).fetchone()
            if u:
                approved_by = str(u["emp_id"])
        return {
            "id": r["ext_id"],
            "employee_id": r["emp_id"],
            "field": r["modify_field"],
            "old_value": r["old_value"],
            "new_value": r["new_value"],
            "reason": r["reason"],
            "status": r["status"],
            "created_at": r["apply_time"],
            "updated_at": r["update_time"],
            "approved_by": approved_by,
            "approved_at": r["approve_time"] or "",
            "rejected_reason": r["reject_reason"] or "",
        }

    def _attendance_rule_to_public(self, r: sqlite3.Row) -> dict:
        return {
            "id": r["id"],
            "name": r["name"],
            "enabled": bool(r["enabled"]),
            "work_type": r["work_type"],
            "priority": int(r["priority"]),
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "center_lat": r["center_lat"],
            "center_lng": r["center_lng"],
            "allowed_radius_m": r["allowed_radius_m"],
            "address_hint": r["address_hint"] or "",
            "updated_at": r["updated_at"],
            "updated_by": r["updated_by"],
            "created_at": r["created_at"],
        }

    def _history_to_public(self, r: sqlite3.Row) -> dict:
        before = None
        after = None
        if r["before_json"]:
            try:
                before = json.loads(r["before_json"])
            except Exception:
                before = None
        if r["after_json"]:
            try:
                after = json.loads(r["after_json"])
            except Exception:
                after = None
        out = {
            "id": r["id"],
            "employee_id": r["employee_id"],
            "changed_by": r["changed_by"],
            "changed_at": r["changed_at"],
            "action": r["action"],
            "before": before,
            "after": after,
        }
        if r["request_id"] is not None:
            out["request_id"] = r["request_id"]
        return out

    def _insert_employee(self, row: dict) -> dict:
        emp_id = str(row.get("employee_id") or row.get("emp_id") or "").strip()
        if not emp_id:
            raise ValueError("employee_id 不能为空")
        name = str(row.get("name") or "").strip()
        department = str(row.get("department") or "").strip()
        position = str(row.get("position") or "").strip()
        if not name or not department or not position:
            raise ValueError("员工姓名/部门/职位不能为空")
        hire_date = str(row.get("hire_date") or "").strip()
        if not hire_date:
            hire_date = date.today().isoformat()
        gender = row.get("gender")
        age = row.get("age")
        phone = row.get("phone")
        work_type = str(row.get("work_type") or "onsite")
        if work_type not in {"onsite", "offsite"}:
            work_type = "onsite"
        active = 1 if bool(row.get("active", True)) else 0
        daily_salary = float(row.get("daily_salary", 0) or 0)
        attendance_days = float(row.get("attendance_days", 0) or 0)
        created_at = str(row.get("created_at") or row.get("create_time") or "")
        updated_at = str(row.get("updated_at") or row.get("update_time") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO employee(emp_id,name,gender,age,department,position,phone,hire_date,daily_salary,attendance_days,work_type,active,create_time,update_time) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    emp_id,
                    name,
                    gender,
                    age,
                    department,
                    position,
                    phone,
                    hire_date,
                    daily_salary,
                    attendance_days,
                    work_type,
                    active,
                    created_at,
                    updated_at,
                ),
            )
        return self.find_one("employees", lambda e: e.get("employee_id") == emp_id) or dict(row)

    def _update_employee(self, row: dict) -> None:
        emp_id = str(row.get("employee_id") or "").strip()
        if not emp_id:
            raise ValueError("employee_id 不能为空")
        updates = {
            "name": row.get("name"),
            "gender": row.get("gender"),
            "age": row.get("age"),
            "department": row.get("department"),
            "position": row.get("position"),
            "phone": row.get("phone"),
            "hire_date": row.get("hire_date"),
            "daily_salary": row.get("daily_salary"),
            "attendance_days": row.get("attendance_days"),
            "work_type": row.get("work_type"),
            "active": 1 if bool(row.get("active", True)) else 0,
        }
        cols = []
        params = []
        for k, v in updates.items():
            if v is None and k in {"name", "department", "position", "hire_date"}:
                continue
            cols.append(f"{k} = ?")
            params.append(v)
        if not cols:
            return
        params.append(emp_id)
        with self._lock:
            self._conn.execute(f"UPDATE employee SET {', '.join(cols)} WHERE emp_id = ?", tuple(params))

    def _insert_user(self, row: dict) -> dict:
        emp_id = str(row.get("user_id") or row.get("emp_id") or "").strip()
        if not emp_id:
            raise ValueError("user_id 不能为空")
        role = str(row.get("role") or "employee")
        if role not in {"admin", "employee"}:
            role = "employee"
        password_hash = str(row.get("password_hash") or "").strip()
        if not password_hash:
            raise ValueError("password_hash 不能为空")
        active = bool(row.get("active", True))
        status = str(row.get("status") or ("active" if active else "inactive"))
        if status not in {"active", "inactive"}:
            status = "active"
        must_change_password = 1 if bool(row.get("must_change_password", False)) else 0
        created_at = str(row.get("created_at") or row.get("create_time") or "")
        updated_at = str(row.get("updated_at") or row.get("update_time") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO user(emp_id,password_hash,role,status,must_change_password,create_time,update_time) "
                "VALUES (?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (emp_id, password_hash, role, status, must_change_password, created_at, updated_at),
            )
        return self.find_one("users", lambda u: u.get("user_id") == emp_id) or dict(row)

    def _update_user(self, row: dict) -> None:
        emp_id = str(row.get("user_id") or "").strip()
        if not emp_id:
            raise ValueError("user_id 不能为空")
        role = row.get("role")
        status = row.get("status")
        if status is None and "active" in row:
            status = "active" if bool(row.get("active", True)) else "inactive"
        must_change_password = row.get("must_change_password")
        cols = []
        params = []
        if role is not None:
            cols.append("role = ?")
            params.append(role)
        if row.get("password_hash") is not None:
            cols.append("password_hash = ?")
            params.append(row.get("password_hash"))
        if status is not None:
            cols.append("status = ?")
            params.append(status)
        if must_change_password is not None:
            cols.append("must_change_password = ?")
            params.append(1 if bool(must_change_password) else 0)
        if not cols:
            return
        params.append(emp_id)
        with self._lock:
            self._conn.execute(f"UPDATE user SET {', '.join(cols)} WHERE emp_id = ?", tuple(params))

    def _ensure_user_id_by_emp_id(self, emp_id: str) -> int | None:
        with self._lock:
            r = self._conn.execute("SELECT user_id FROM user WHERE emp_id = ?", (emp_id,)).fetchone()
        if not r:
            return None
        return int(r["user_id"])

    def _insert_overtime(self, row: dict) -> dict:
        ext_id = str(row.get("id") or row.get("ext_id") or "").strip() or str(uuid.uuid4())
        emp_id = str(row.get("employee_id") or row.get("emp_id") or "").strip()
        if not emp_id:
            raise ValueError("employee_id 不能为空")
        created_at = str(row.get("created_at") or row.get("apply_time") or "")
        start_date = str(row.get("start_date") or row.get("date") or "").strip()
        end_date = str(row.get("end_date") or row.get("start_date") or row.get("date") or "").strip()
        if not start_date and created_at:
            start_date = str(created_at)[:10]
        if not end_date:
            end_date = start_date
        days = float(row.get("days", 0) or 0)
        reason = str(row.get("reason") or "").strip()
        if not reason:
            raise ValueError("reason 不能为空")
        status = str(row.get("status") or "pending")
        if status not in {"pending", "approved", "rejected"}:
            status = "pending"
        approved_by = str(row.get("approved_by") or "").strip()
        approver_id = self._ensure_user_id_by_emp_id(approved_by) if approved_by else None
        approved_at = str(row.get("approved_at") or row.get("approve_time") or "") or None
        rejected_reason = str(row.get("rejected_reason") or row.get("reject_reason") or "") or None
        updated_at = str(row.get("updated_at") or row.get("update_time") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO overtime(ext_id,emp_id,start_date,end_date,days,reason,apply_time,status,approver_id,approve_time,reject_reason,update_time) "
                "VALUES (?,?,?,?,?, ?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    ext_id,
                    emp_id,
                    start_date,
                    end_date,
                    days,
                    reason,
                    created_at,
                    status,
                    approver_id,
                    approved_at,
                    rejected_reason,
                    updated_at,
                ),
            )
        return self.find_one("overtime_requests", lambda r: r.get("id") == ext_id) or dict(row)

    def _update_overtime(self, row: dict) -> None:
        ext_id = str(row.get("id") or "").strip()
        if not ext_id:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        if row.get("status") is not None:
            cols.append("status = ?")
            params.append(row.get("status"))
        if row.get("start_date") is not None:
            cols.append("start_date = ?")
            params.append(str(row.get("start_date") or ""))
        if row.get("end_date") is not None:
            cols.append("end_date = ?")
            params.append(str(row.get("end_date") or ""))
        if row.get("days") is not None:
            cols.append("days = ?")
            params.append(float(row.get("days") or 0))
        if row.get("reason") is not None:
            cols.append("reason = ?")
            params.append(row.get("reason"))
        if row.get("approved_at") is not None:
            cols.append("approve_time = ?")
            params.append(row.get("approved_at") or None)
        if row.get("rejected_reason") is not None:
            cols.append("reject_reason = ?")
            params.append(row.get("rejected_reason") or None)
        if row.get("approved_by") is not None:
            approved_by = str(row.get("approved_by") or "").strip()
            approver_id = self._ensure_user_id_by_emp_id(approved_by) if approved_by else None
            cols.append("approver_id = ?")
            params.append(approver_id)
        if row.get("created_at") is not None:
            cols.append("apply_time = ?")
            params.append(row.get("created_at"))
        if row.get("updated_at") is not None:
            cols.append("update_time = ?")
            params.append(row.get("updated_at"))
        if not cols:
            return
        params.append(ext_id)
        with self._lock:
            self._conn.execute(f"UPDATE overtime SET {', '.join(cols)} WHERE ext_id = ?", tuple(params))

    def _insert_check_record(self, row: dict) -> dict:
        ext_id = str(row.get("id") or row.get("ext_id") or "").strip() or str(uuid.uuid4())
        emp_id = str(row.get("employee_id") or row.get("emp_id") or "").strip()
        if not emp_id:
            raise ValueError("employee_id 不能为空")
        ts = str(row.get("ts") or row.get("check_time") or "").strip()
        if not ts:
            raise ValueError("ts 不能为空")
        status = str(row.get("status") or row.get("check_status") or "present")
        status_db = "normal" if status == "present" else status
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO check_record(ext_id,emp_id,check_time,check_status,check_location,source,address,lat,lng,rule_id,create_time) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    ext_id,
                    emp_id,
                    ts,
                    status_db,
                    row.get("check_location"),
                    row.get("source"),
                    row.get("address"),
                    row.get("lat"),
                    row.get("lng"),
                    row.get("rule_id"),
                    str(row.get("created_at") or row.get("create_time") or ""),
                ),
            )
        return self.find_one("attendance_records", lambda r: r.get("id") == ext_id) or dict(row)

    def _update_check_record(self, row: dict) -> None:
        ext_id = str(row.get("id") or "").strip()
        if not ext_id:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        if row.get("ts") is not None:
            cols.append("check_time = ?")
            params.append(row.get("ts"))
        if row.get("status") is not None:
            status = str(row.get("status") or "")
            status_db = "normal" if status == "present" else status
            cols.append("check_status = ?")
            params.append(status_db)
        for k_db, k in [
            ("source", "source"),
            ("address", "address"),
            ("lat", "lat"),
            ("lng", "lng"),
            ("rule_id", "rule_id"),
            ("check_location", "check_location"),
        ]:
            if row.get(k) is not None:
                cols.append(f"{k_db} = ?")
                params.append(row.get(k))
        if not cols:
            return
        params.append(ext_id)
        with self._lock:
            self._conn.execute(f"UPDATE check_record SET {', '.join(cols)} WHERE ext_id = ?", tuple(params))

    def _insert_modify_apply(self, row: dict) -> dict:
        ext_id = str(row.get("id") or row.get("ext_id") or "").strip() or str(uuid.uuid4())
        emp_id = str(row.get("employee_id") or row.get("emp_id") or "").strip()
        if not emp_id:
            raise ValueError("employee_id 不能为空")
        field = str(row.get("field") or row.get("modify_field") or "").strip()
        if not field:
            raise ValueError("field 不能为空")
        new_value = str(row.get("new_value") or "").strip()
        if not new_value:
            raise ValueError("new_value 不能为空")
        reason = str(row.get("reason") or "").strip()
        if not reason:
            raise ValueError("reason 不能为空")
        status = str(row.get("status") or "pending")
        if status not in {"pending", "approved", "rejected"}:
            status = "pending"
        approved_by = str(row.get("approved_by") or "").strip()
        approver_id = self._ensure_user_id_by_emp_id(approved_by) if approved_by else None
        created_at = str(row.get("created_at") or row.get("apply_time") or "")
        approved_at = str(row.get("approved_at") or row.get("approve_time") or "") or None
        rejected_reason = str(row.get("rejected_reason") or row.get("reject_reason") or "") or None
        updated_at = str(row.get("updated_at") or row.get("update_time") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO info_modify_apply(ext_id,emp_id,modify_field,old_value,new_value,reason,apply_time,status,approver_id,approve_time,reject_reason,update_time) "
                "VALUES (?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    ext_id,
                    emp_id,
                    field,
                    row.get("old_value"),
                    new_value,
                    reason,
                    created_at,
                    status,
                    approver_id,
                    approved_at,
                    rejected_reason,
                    updated_at,
                ),
            )
        return self.find_one("employee_change_requests", lambda r: r.get("id") == ext_id) or dict(row)

    def _update_modify_apply(self, row: dict) -> None:
        ext_id = str(row.get("id") or "").strip()
        if not ext_id:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        if row.get("status") is not None:
            cols.append("status = ?")
            params.append(row.get("status"))
        if row.get("field") is not None:
            cols.append("modify_field = ?")
            params.append(row.get("field"))
        if row.get("old_value") is not None:
            cols.append("old_value = ?")
            params.append(row.get("old_value"))
        if row.get("new_value") is not None:
            cols.append("new_value = ?")
            params.append(row.get("new_value"))
        if row.get("reason") is not None:
            cols.append("reason = ?")
            params.append(row.get("reason"))
        if row.get("approved_at") is not None:
            cols.append("approve_time = ?")
            params.append(row.get("approved_at") or None)
        if row.get("rejected_reason") is not None:
            cols.append("reject_reason = ?")
            params.append(row.get("rejected_reason") or None)
        if row.get("approved_by") is not None:
            approved_by = str(row.get("approved_by") or "").strip()
            approver_id = self._ensure_user_id_by_emp_id(approved_by) if approved_by else None
            cols.append("approver_id = ?")
            params.append(approver_id)
        if row.get("created_at") is not None:
            cols.append("apply_time = ?")
            params.append(row.get("created_at"))
        if row.get("updated_at") is not None:
            cols.append("update_time = ?")
            params.append(row.get("updated_at"))
        if not cols:
            return
        params.append(ext_id)
        with self._lock:
            self._conn.execute(f"UPDATE info_modify_apply SET {', '.join(cols)} WHERE ext_id = ?", tuple(params))

    def _insert_salary_record(self, row: dict) -> dict:
        rid = str(row.get("id") or "").strip() or str(uuid.uuid4())
        employee_id = str(row.get("employee_id") or "").strip()
        month = str(row.get("month") or "").strip()
        if not employee_id or not month:
            raise ValueError("employee_id/month 不能为空")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO salary_record(id,employee_id,month,attendance_days,overtime_days,daily_salary,total_salary,calculated_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    rid,
                    employee_id,
                    month,
                    float(row.get("attendance_days", 0) or 0),
                    float(row.get("overtime_days", 0) or 0),
                    float(row.get("daily_salary", 0) or 0),
                    float(row.get("total_salary", 0) or 0),
                    str(row.get("calculated_at") or now_iso()),
                    str(row.get("updated_at") or ""),
                ),
            )
        out = dict(row)
        out["id"] = rid
        return out

    def _update_salary_record(self, row: dict) -> None:
        rid = str(row.get("id") or "").strip()
        employee_id = str(row.get("employee_id") or "").strip()
        month = str(row.get("month") or "").strip()
        cols = []
        params = []
        for k in ["attendance_days", "overtime_days", "daily_salary", "total_salary", "calculated_at", "updated_at"]:
            if k in row and row.get(k) is not None:
                col = "updated_at" if k == "updated_at" else k
                cols.append(f"{col} = ?")
                params.append(row.get(k))
        if not cols:
            return
        if rid:
            params.append(rid)
            where = "id = ?"
        else:
            params.extend([employee_id, month])
            where = "employee_id = ? AND month = ?"
        with self._lock:
            self._conn.execute(f"UPDATE salary_record SET {', '.join(cols)} WHERE {where}", tuple(params))

    def _insert_attendance_manual_override(self, row: dict) -> dict:
        rid = str(row.get("id") or "").strip()
        employee_id = str(row.get("employee_id") or "").strip()
        month = str(row.get("month") or "").strip()
        if not rid or not employee_id or not month:
            raise ValueError("id/employee_id/month 不能为空")
        updated_by = str(row.get("updated_by") or "").strip()
        if not updated_by:
            raise ValueError("updated_by 不能为空")
        updated_at = str(row.get("updated_at") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO attendance_manual_override(id,employee_id,month,attendance_days,updated_by,updated_at) "
                "VALUES (?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    rid,
                    employee_id,
                    month,
                    float(row.get("attendance_days", 0) or 0),
                    updated_by,
                    updated_at,
                ),
            )
        return dict(row)

    def _update_attendance_manual_override(self, row: dict) -> None:
        rid = str(row.get("id") or "").strip()
        if not rid:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        for key in ["employee_id", "month", "attendance_days", "updated_by", "updated_at"]:
            if key in row and row.get(key) is not None:
                cols.append(f"{key} = ?")
                params.append(row.get(key))
        if not cols:
            return
        params.append(rid)
        with self._lock:
            self._conn.execute(f"UPDATE attendance_manual_override SET {', '.join(cols)} WHERE id = ?", tuple(params))

    def _insert_attendance_rule(self, row: dict) -> dict:
        rid = str(row.get("id") or "").strip() or str(uuid.uuid4())
        name = str(row.get("name") or "默认规则")
        enabled = 1 if bool(row.get("enabled", False)) else 0
        work_type = str(row.get("work_type") or "onsite")
        if work_type not in {"onsite", "offsite"}:
            work_type = "onsite"
        priority = int(row.get("priority", 100) or 100)
        start_time = str(row.get("start_time") or "09:00")
        end_time = str(row.get("end_time") or "10:00")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO attendance_rule(id,name,enabled,work_type,priority,start_time,end_time,center_lat,center_lng,allowed_radius_m,address_hint,updated_at,updated_by,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))",
                (
                    rid,
                    name,
                    enabled,
                    work_type,
                    priority,
                    start_time,
                    end_time,
                    row.get("center_lat"),
                    row.get("center_lng"),
                    row.get("allowed_radius_m"),
                    row.get("address_hint") or "",
                    str(row.get("updated_at") or now_iso()),
                    str(row.get("updated_by") or ""),
                    str(row.get("created_at") or ""),
                ),
            )
        return self.find_one("attendance_rules", lambda r: r.get("id") == rid) or dict(row)

    def _update_attendance_rule(self, row: dict) -> None:
        rid = str(row.get("id") or "").strip()
        if not rid:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        for key, col in [
            ("name", "name"),
            ("enabled", "enabled"),
            ("work_type", "work_type"),
            ("priority", "priority"),
            ("start_time", "start_time"),
            ("end_time", "end_time"),
            ("center_lat", "center_lat"),
            ("center_lng", "center_lng"),
            ("allowed_radius_m", "allowed_radius_m"),
            ("address_hint", "address_hint"),
            ("updated_at", "updated_at"),
            ("updated_by", "updated_by"),
        ]:
            if key in row and row.get(key) is not None:
                v = row.get(key)
                if key == "enabled":
                    v = 1 if bool(v) else 0
                cols.append(f"{col} = ?")
                params.append(v)
        if not cols:
            return
        params.append(rid)
        with self._lock:
            self._conn.execute(f"UPDATE attendance_rule SET {', '.join(cols)} WHERE id = ?", tuple(params))

    def _insert_history(self, row: dict) -> dict:
        rid = str(row.get("id") or "").strip() or str(uuid.uuid4())
        employee_id = str(row.get("employee_id") or "").strip()
        changed_by = str(row.get("changed_by") or "").strip()
        changed_at = str(row.get("changed_at") or now_iso())
        action = str(row.get("action") or "").strip()
        if not employee_id or not changed_by or not action:
            raise ValueError("employee_id/changed_by/action 不能为空")
        before_json = None
        after_json = None
        if "before" in row:
            before_json = json.dumps(row.get("before"), ensure_ascii=False)
        if "after" in row:
            after_json = json.dumps(row.get("after"), ensure_ascii=False)
        request_id = row.get("request_id")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO employee_change_history(id,employee_id,changed_by,changed_at,action,before_json,after_json,request_id) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (rid, employee_id, changed_by, changed_at, action, before_json, after_json, request_id),
            )
        return self.find_one("employee_change_history", lambda r: r.get("id") == rid) or dict(row)

    def _update_history(self, row: dict) -> None:
        rid = str(row.get("id") or "").strip()
        if not rid:
            raise ValueError("id 不能为空")
        cols = []
        params = []
        for key, col in [
            ("employee_id", "employee_id"),
            ("changed_by", "changed_by"),
            ("changed_at", "changed_at"),
            ("action", "action"),
            ("request_id", "request_id"),
        ]:
            if key in row and row.get(key) is not None:
                cols.append(f"{col} = ?")
                params.append(row.get(key))
        if "before" in row:
            cols.append("before_json = ?")
            params.append(json.dumps(row.get("before"), ensure_ascii=False))
        if "after" in row:
            cols.append("after_json = ?")
            params.append(json.dumps(row.get("after"), ensure_ascii=False))
        if not cols:
            return
        params.append(rid)
        with self._lock:
            self._conn.execute(f"UPDATE employee_change_history SET {', '.join(cols)} WHERE id = ?", tuple(params))

    def _insert_system_config(self, row: dict) -> dict:
        key = str(row.get("key") or "").strip()
        value = str(row.get("value") or "").strip()
        if not key:
            raise ValueError("key 不能为空")
        if value == "":
            raise ValueError("value 不能为空")
        updated_at = str(row.get("updated_at") or "")
        updated_by = str(row.get("updated_by") or "")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO system_config(key,value,updated_at,updated_by) VALUES (?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),?)",
                (key, value, updated_at, updated_by),
            )
        return {"key": key, "value": value, "updated_at": updated_at or now_iso(), "updated_by": updated_by}

    def _update_system_config(self, row: dict) -> None:
        key = str(row.get("key") or "").strip()
        if not key:
            raise ValueError("key 不能为空")
        cols = []
        params = []
        if row.get("value") is not None:
            cols.append("value = ?")
            params.append(row.get("value"))
        if row.get("updated_at") is not None:
            cols.append("updated_at = ?")
            params.append(row.get("updated_at"))
        if row.get("updated_by") is not None:
            cols.append("updated_by = ?")
            params.append(row.get("updated_by"))
        if not cols:
            return
        params.append(key)
        with self._lock:
            self._conn.execute(f"UPDATE system_config SET {', '.join(cols)} WHERE key = ?", tuple(params))

    def _insert_system_log(self, row: dict) -> dict:
        user_emp_id = str(row.get("user_id") or "").strip()
        if not user_emp_id:
            raise ValueError("user_id 不能为空")
        uid = self._ensure_user_id_by_emp_id(user_emp_id)
        if uid is None:
            raise ValueError("user_id 不存在")
        op_type = str(row.get("operation_type") or "").strip()
        op_content = str(row.get("operation_content") or "").strip()
        if not op_type or not op_content:
            raise ValueError("operation_type/operation_content 不能为空")
        op_ip = row.get("operation_ip")
        op_result = str(row.get("operation_result") or "success")
        if op_result not in {"success", "failed"}:
            op_result = "success"
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO system_log(user_id,operation_type,operation_content,operation_ip,operation_result) VALUES (?,?,?,?,?)",
                (uid, op_type, op_content, op_ip, op_result),
            )
            log_id = int(cur.lastrowid)
        out = dict(row)
        out["log_id"] = log_id
        return out
