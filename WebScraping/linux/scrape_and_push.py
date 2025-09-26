#!/usr/bin/env python3
# 1) Pre-push local Data -> S3 (skip overwrites)
# 2) Run scraper (manifest-first, only missing)
# 3) Post-push new files -> S3 (skip overwrites)
# Minimal, informative logs.

import argparse, sys, shutil, subprocess, shlex
from pathlib import Path
from typing import List, Tuple, Optional

EXCLUDES = ["_tmp_worker_*/**", "**/.DS_Store", "*.tmp"]

def die(msg: str, code: int = 2):
    print(f"[error] {msg}", file=sys.stderr); sys.exit(code)

def list_payload(root: Path) -> List[str]:
    files: List[str] = []
    if root.exists():
        for p in root.rglob("*"):
            if p.is_file():
                rel = p.relative_to(root).as_posix()
                if rel.startswith("_tmp_worker_") or rel.endswith(".tmp") or rel.endswith(".DS_Store"):
                    continue
                files.append(rel)
    return sorted(files)

def run_capture(cmd: list[str], env: Optional[dict] = None) -> Tuple[int, str]:
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    out_chunks = []
    assert proc.stdout is not None
    for line in proc.stdout:
        out_chunks.append(line)
    rc = proc.wait()
    return rc, "".join(out_chunks)

def rclone_lsf(remote_uri: str, rclone_bin: str, extra: Optional[list[str]] = None) -> List[str]:
    cmd = [rclone_bin, "lsf", remote_uri, "--recursive", "--fast-list"]
    if extra: cmd += extra
    rc, out = run_capture(cmd)
    if rc not in (0, None): return []
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    return [ln for ln in lines if not ln.endswith("/")]

def rclone_copy(src_dir: str, dst_uri: str, rclone_bin: str) -> int:
    cmd = [
        rclone_bin, "copy", src_dir, dst_uri,
        "--ignore-existing",
        "--transfers", "16",
        "--checkers", "32",
        "--contimeout", "15s",
        "--timeout", "30m",
        "--retries", "3",
        "--low-level-retries", "5",
        "--s3-no-check-bucket",
        "--no-traverse",
    ]
    for pat in EXCLUDES:
        cmd += ["--exclude", pat]
    # quiet: no -P, no command echo
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT).returncode

def main():
    ap = argparse.ArgumentParser(description="Pre-push, scrape, post-push (no overwrites).")
    ap.add_argument("wells_file", help="Path to wells subset (e.g., wells_parts/wells_10.txt)")
    ap.add_argument("--scraper", default="scraping.py", help="Scraper script path")
    ap.add_argument("--python", default=sys.executable, help="Python interpreter for scraper")
    ap.add_argument("--out-base", default="Data", help="Local output directory for scraped files")

    # pass-through to scraper
    ap.add_argument("--workers", type=int, default=2, help="Scraper workers")
    ap.add_argument("--dashboards", default="all", help="Comma list or 'all'")
    ap.add_argument("--headless", action="store_true", help="Run scraper headless")
    ap.add_argument("--force", action="store_true", help="Force re-download (not recommended)")

    # rclone target
    ap.add_argument("--remote", default="s3aer", help="rclone remote name")
    ap.add_argument("--bucket", required=True, help="S3 bucket name")
    ap.add_argument("--prefix", default="Data", help="S3 folder/prefix (e.g., 'Data')")
    ap.add_argument("--rclone", default="rclone", help="rclone binary")
    args = ap.parse_args()

    if not shutil.which(args.rclone): die("rclone not found in PATH")
    if not shutil.which(args.python): die(f"python not found: {args.python}")
    if not Path(args.scraper).is_file(): die(f"scraper not found: {args.scraper}")
    if not Path(args.wells_file).is_file(): die(f"wells file not found: {args.wells_file}")

    out_base = Path(args.out_base).resolve()
    out_base.mkdir(parents=True, exist_ok=True)
    local_files = list_payload(out_base)

    dest = f"{args.remote}:{args.bucket}/{args.prefix}"

    # --- PRE-PUSH SUMMARY ---
    remote_before = set(rclone_lsf(dest, args.rclone))
    local_set = set(local_files)
    dismissed = len([p for p in local_set if p in remote_before])
    will_upload = len(local_set) - dismissed
    print(f"[info] upload(pre): dismissed existing={dismissed}, will upload new={will_upload}")

    # --- PRE-PUSH ---
    if will_upload > 0:
        rc = rclone_copy(str(out_base), dest, args.rclone)
        if rc not in (0, None): die(f"pre-push failed with code {rc}", code=rc)
        remote_after = set(rclone_lsf(dest, args.rclone))
        actually_uploaded = len([p for p in local_set if p not in remote_before and p in remote_after])
        print(f"[info] uploaded(pre): {actually_uploaded} new")
    else:
        print("[info] nothing to upload pre-scrape.")

    # --- RUN SCRAPER ---
    cmd = [
        args.python, args.scraper,
        "--workers", str(args.workers),
        "--wells", args.wells_file,
        "--out-base", str(out_base),
        "--dashboards", args.dashboards,
    ]
    if args.headless: cmd.append("--headless")
    if args.force:    cmd.append("--force")
    print("[info] run:", " ".join(shlex.quote(c) for c in cmd))
    res = subprocess.run(cmd)
    if res.returncode not in (0, None): sys.exit(res.returncode)

    # refresh local listing (may have new files)
    local_files = list_payload(out_base); local_set = set(local_files)

    # --- POST-PUSH SUMMARY ---
    remote_before = set(rclone_lsf(dest, args.rclone))
    dismissed = len([p for p in local_set if p in remote_before])
    will_upload = len(local_set) - dismissed
    print(f"[info] upload(post): dismissed existing={dismissed}, will upload new={will_upload}")

    # --- POST-PUSH ---
    if will_upload > 0:
        rc2 = rclone_copy(str(out_base), dest, args.rclone)
        if rc2 not in (0, None): die(f"post-push failed with code {rc2}", code=rc2)
        remote_after = set(rclone_lsf(dest, args.rclone))
        actually_uploaded = len([p for p in local_set if p not in remote_before and p in remote_after])
        print(f"[info] uploaded(post): {actually_uploaded} new")
    else:
        print("[info] nothing to upload post-scrape.")

    print("[done] complete.")

if __name__ == "__main__":
    main()
