# s3_merge.py
# Minimal rclone helpers: read/write text, existence, upload-if-new, list prefix.

import json, os, subprocess
from pathlib import Path
from typing import Optional, List

REMOTE = os.environ.get("AER_REMOTE", "aer:aer-scrape-prod")

def _rcmd(*args, input: Optional[str] = None) -> subprocess.CompletedProcess:
    return subprocess.run(["rclone", *args], text=True, input=input,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

def s3_lsjson(remote_key: str):
    p = _rcmd("lsjson", f"{REMOTE}/{remote_key}")
    if p.returncode != 0:
        return []
    try:
        return json.loads(p.stdout or "[]")
    except Exception:
        return []

def s3_exists(remote_key: str) -> bool:
    parent = str(Path(remote_key).parent).replace("\\", "/")
    name   = Path(remote_key).name
    arr = s3_lsjson(parent)
    return any((not o.get("IsDir", False)) and (o.get("Path","") == name) for o in arr)

def s3_copyto_if_new(local_file: Path, remote_key: str) -> bool:
    if s3_exists(remote_key):
        return False
    p = _rcmd("copyto", "--ignore-existing", str(local_file), f"{REMOTE}/{remote_key}")
    return p.returncode == 0

def s3_read_text(remote_key: str) -> Optional[str]:
    p = subprocess.run(["rclone", "cat", f"{REMOTE}/{remote_key}"],
                       text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return p.stdout if p.returncode == 0 else None

def s3_put_text(remote_key: str, text: str) -> bool:
    p = _rcmd("rcat", f"{REMOTE}/{remote_key}", input=text or "")
    return p.returncode == 0

def s3_list_prefix(prefix: str) -> List[str]:
    arr = s3_lsjson(prefix)
    return [o.get("Path","") for o in arr if not o.get("IsDir", False)]
