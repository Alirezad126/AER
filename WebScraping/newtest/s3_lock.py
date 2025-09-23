# s3_lock.py
# Simple S3/rclone locks with TTL + heartbeat + purge.

import json, os, subprocess, time, socket, threading
from datetime import datetime, timezone
from urllib.parse import quote

REMOTE = os.environ.get("AER_REMOTE", "aer:aer-scrape-prod")
LOCK_TTL_SECONDS = int(os.environ.get("AER_LOCK_TTL_SEC", "3600"))
HEARTBEAT_SEC    = int(os.environ.get("AER_LOCK_HEARTBEAT_SEC", "120"))

def _lsjson_any(path: str):
    p = subprocess.run(["rclone", "lsjson", f"{REMOTE}/{path}"],
                       text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    if p.returncode != 0: return []
    try: return json.loads(p.stdout or "[]")
    except Exception: return []

def _delete(key: str):
    subprocess.run(["rclone", "deletefile", f"{REMOTE}/{key}"],
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)

def purge_expired_locks():
    arr = _lsjson_any("locks")
    now = time.time()
    for o in arr:
        if o.get("IsDir", False): continue
        name = o.get("Path",""); m = o.get("ModTime","")
        try: t = datetime.fromisoformat(m.replace("Z","+00:00")).timestamp()
        except Exception: t = now
        if (now - t) > LOCK_TTL_SECONDS:
            _delete(f"locks/{name}")

def _key(uwi_entry: str) -> str:
    enc = quote((uwi_entry or "").strip(), safe="")
    return f"locks/{enc}.lock"

def acquire_lock(uwi: str) -> bool:
    purge_expired_locks()
    key = _key(uwi)
    arr = _lsjson_any(key)
    now = time.time()
    if arr:
        try:
            t = datetime.fromisoformat(arr[0]["ModTime"].replace("Z","+00:00")).timestamp()
        except Exception:
            t = now
        if (now - t) < LOCK_TTL_SECONDS:
            return False
        _delete(key)  # stale
    payload = json.dumps({
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": datetime.now(timezone.utc).isoformat()
    }, indent=2)
    p = subprocess.run(["rclone", "rcat", f"{REMOTE}/{key}"],
                       input=payload, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return p.returncode == 0

def release_lock(uwi: str):
    _delete(_key(uwi))

class _HB:
    def __init__(self, uwi: str):
        self.uwi = uwi
        self.stop_flag = False
        self.t = threading.Thread(target=self._loop, daemon=True); self.t.start()
    def _loop(self):
        while not self.stop_flag:
            try:
                subprocess.run(["rclone", "touch", f"{REMOTE}/{_key(self.uwi)}"],
                               check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
            except Exception: pass
            time.sleep(HEARTBEAT_SEC)
    def stop(self):
        self.stop_flag = True
        try: self.t.join(timeout=2)
        except Exception: pass

def start_lock_heartbeat(uwi: str):
    return _HB(uwi)
