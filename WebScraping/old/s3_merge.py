# s3_merge.py
import json, os, subprocess
from pathlib import Path

REMOTE = os.environ.get("AER_REMOTE", "aer:aer-scrape-prod")

def _rcmd(*args, input=None):
    return subprocess.run(["rclone", *args], text=True, input=input,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def s3_lsjson(remote_key: str):
    """Return lsjson array for a path; [] if empty; None on error."""
    p = _rcmd("lsjson", f"{REMOTE}/{remote_key}")
    if p.returncode != 0:
        # rclone returns non-zero if the path doesn't exist; treat as empty
        return [] if "directory not found" in p.stdout.lower() else []
    try:
        return json.loads(p.stdout or "[]")
    except Exception:
        return []

def s3_exists(remote_key: str) -> bool:
    """True if object exists at exact key (file)."""
    arr = s3_lsjson(remote_key)
    return any(not o.get("IsDir", False) and o.get("Path", "") == Path(remote_key).name for o in arr)

def s3_any_under(prefix: str) -> bool:
    """True if there is at least one object under prefix/"""
    arr = s3_lsjson(prefix)
    return any(not o.get("IsDir", False) for o in arr)

def s3_copyto_if_new(local_file: Path, remote_key: str) -> bool:
    """
    Upload single file to S3 key if it doesn't already exist.
    Returns True if uploaded, False if skipped or failed.
    """
    # Fast existence check
    if s3_exists(remote_key):
        return False
    p = _rcmd("copyto", "--ignore-existing", str(local_file), f"{REMOTE}/{remote_key}")
    return p.returncode == 0

def s3_delete(remote_key: str):
    _rcmd("deletefile", f"{REMOTE}/{remote_key}")
