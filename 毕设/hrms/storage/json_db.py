import json
import os
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

