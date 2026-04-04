import json
import os
import subprocess
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Global lock for safe file writing
FILE_LOCK = threading.Lock()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _count_existing_pdfs_by_domain(out_dir: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not os.path.isdir(out_dir):
        return counts

    try:
        entries = list(os.scandir(out_dir))
    except OSError:
        return counts

    for entry in entries:
        if not entry.is_dir():
            continue
        domain = entry.name
        total = 0
        for root, _dirs, files in os.walk(entry.path):
            total += sum(1 for name in files if str(name).lower().endswith(".pdf"))
        if total > 0:
            counts[domain] = total
    return counts

def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _git_commit() -> Optional[str]:
    try:
        value = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        return value or None
    except Exception:
        return None

def _read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _write_json(path: str, payload: Any) -> None:
    tmp = f"{path}.tmp"
    with FILE_LOCK:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp, path)

def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    with FILE_LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True) + "\n")

def _write_jsonl_atomic(path: str, rows: List[Dict[str, Any]]) -> None:
    tmp = f"{path}.tmp"
    with FILE_LOCK:
        with open(tmp, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, sort_keys=True) + "\n")
        os.replace(tmp, path)

def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows
