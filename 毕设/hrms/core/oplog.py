import json
import os
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_oplog(log_dir: str, record: dict) -> None:
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, "operation_logs.jsonl")
    record = dict(record)
    record.setdefault("ts", _now_iso())
    line = json.dumps(record, ensure_ascii=False)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

