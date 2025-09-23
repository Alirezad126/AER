# s3_lock.py
import json, os, subprocess, time, socket, threading
from datetime import datetime, timezone
from urllib.parse import quote   # <-- add this
from typing import List, Optional, Dict, Tuple



REMOTE = os.environ.get("AER_REMOTE", "aer:aer-scrape-prod")
LOCK_TTL_SECONDS = int(os.environ.get("AER_LOCK_TTL_SEC", str(1*60*60)))
HEARTBEAT_SEC    = int(os.environ.get("AER_LOCK_HEARTBEAT_SEC", "120"))

def _key(uwi_entry: str) -> str:
    # Encode slashes etc. so the S3 object is a single file under locks/
    encoded = quote((uwi_entry or "").strip(), safe="")
    return f"locks/{encoded}.lock"


def _lsjson(remote_key: str):
    try:
        out = subprocess.check_output(
            ["rclone", "lsjson", f"{REMOTE}/{remote_key}"],
            stderr=subprocess.STDOUT, text=True
        )
        arr = json.loads(out)
        return arr[0] if arr else None
    except subprocess.CalledProcessError:
        return None

def acquire_lock(uwi: str) -> bool:
    key = _key(uwi)
    meta = _lsjson(key)
    now = time.time()
    if meta:
        try:
            # Use the module-level datetime import
            mtime = datetime.fromisoformat(meta["ModTime"].replace("Z", "+00:00")).timestamp()
        except Exception:
            mtime = now
        if (now - mtime) < LOCK_TTL_SECONDS:
            return False  # fresh lock exists

    payload = json.dumps({
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": datetime.now(timezone.utc).isoformat()  # OK now
    }, indent=2)

    p = subprocess.run(["rclone", "rcat", f"{REMOTE}/{key}"], input=payload, text=True)
    return p.returncode == 0

def release_lock(uwi: str):
    key = _key(uwi)
    subprocess.run(["rclone", "deletefile", f"{REMOTE}/{key}"], check=False)

def touch_lock(uwi: str):
    key = _key(uwi)
    subprocess.run(["rclone", "touch", f"{REMOTE}/{key}"], check=False)

def start_lock_heartbeat(uwi: str):
    """Keep the lock fresh in the background."""
    def _loop():
        while True:
            try:
                touch_lock(uwi)
            except Exception:
                pass
            time.sleep(HEARTBEAT_SEC)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t
