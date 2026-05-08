import json
import os
import sqlite3
import threading
from datetime import datetime, timezone


_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def _get_lock(path: str) -> threading.Lock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(path)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[path] = lock
        return lock


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonDB:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        _ensure_dir(self.base_dir)

    def table_path(self, name: str) -> str:
        return os.path.join(self.base_dir, f"{name}.json")

    def read_all(self, name: str) -> list[dict]:
        path = self.table_path(name)
        lock = _get_lock(path)
        with lock:
            if not os.path.exists(path):
                return []
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                return []

    def write_all(self, name: str, rows: list[dict]) -> None:
        path = self.table_path(name)
        lock = _get_lock(path)
        with lock:
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)

    def insert(self, name: str, row: dict) -> dict:
        rows = self.read_all(name)
        rows.append(row)
        self.write_all(name, rows)
        return row

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
        new_rows = []
        for row in rows:
            if changed is None and predicate(row):
                new_row = updater(dict(row))
                changed = new_row
                new_rows.append(new_row)
            else:
                new_rows.append(row)
        if changed is None:
            return None
        self.write_all(name, new_rows)
        return changed

    def delete_one(self, name: str, predicate) -> bool:
        rows = self.read_all(name)
        new_rows = [row for row in rows if not predicate(row)]
        if len(new_rows) == len(rows):
            return False
        self.write_all(name, new_rows)
        return True


class SQLiteDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_rows (
                    pk INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    data_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_kv_rows_table_name ON kv_rows(table_name)")
            self._conn.commit()

    def read_all(self, name: str) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT data_json FROM kv_rows WHERE table_name = ? ORDER BY pk ASC",
                (name,),
            )
            rows = cur.fetchall()
        out: list[dict] = []
        for r in rows:
            try:
                obj = json.loads(r["data_json"])
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
        return out

    def insert(self, name: str, row: dict) -> dict:
        data_json = json.dumps(row, ensure_ascii=False)
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv_rows(table_name, data_json) VALUES(?, ?)",
                (name, data_json),
            )
            self._conn.commit()
        return row

    def find_one(self, name: str, predicate) -> dict | None:
        for row in self.read_all(name):
            if predicate(row):
                return row
        return None

    def find_many(self, name: str, predicate) -> list[dict]:
        return [row for row in self.read_all(name) if predicate(row)]

    def update_one(self, name: str, predicate, updater) -> dict | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT pk, data_json FROM kv_rows WHERE table_name = ? ORDER BY pk ASC",
                (name,),
            )
            rows = cur.fetchall()
            target_pk = None
            target_obj = None
            for r in rows:
                try:
                    obj = json.loads(r["data_json"])
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                if predicate(obj):
                    target_pk = int(r["pk"])
                    target_obj = obj
                    break
            if target_pk is None or target_obj is None:
                return None
            new_obj = updater(dict(target_obj))
            data_json = json.dumps(new_obj, ensure_ascii=False)
            self._conn.execute("UPDATE kv_rows SET data_json = ? WHERE pk = ?", (data_json, target_pk))
            self._conn.commit()
            return new_obj

    def delete_one(self, name: str, predicate) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "SELECT pk, data_json FROM kv_rows WHERE table_name = ? ORDER BY pk ASC",
                (name,),
            )
            rows = cur.fetchall()
            target_pk = None
            for r in rows:
                try:
                    obj = json.loads(r["data_json"])
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                if predicate(obj):
                    target_pk = int(r["pk"])
                    break
            if target_pk is None:
                return False
            self._conn.execute("DELETE FROM kv_rows WHERE pk = ?", (target_pk,))
            self._conn.commit()
            return True

    def close(self) -> None:
        try:
            with self._lock:
                self._conn.close()
        except Exception:
            pass
