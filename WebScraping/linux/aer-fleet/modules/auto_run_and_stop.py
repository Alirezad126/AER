# modules/auto_run_and_stop.py
import argparse, time, json, boto3, botocore
from typing import Dict, List
from .config import (
    REGION, BUCKET, S3_LOG_PREFIX, ROLE_TAG,
    CODE_DIR, OUT_BASE, REMOTE_NAME, PREFIX, DASHBOARDS
)

ec2 = boto3.client("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

WRAPPER = r"""#!/usr/bin/env bash
set -euo pipefail

REGION="{region}"
BUCKET="{bucket}"
S3_PREFIX="{s3_prefix}"
CODE_DIR="{code_dir}"
OUT_BASE="{out_base}"
REMOTE="{remote}"
PREFIX="{prefix}"
DASH="{dash}"
SHUT_MODE="{shutdown}"

export RCLONE_CONFIG=/etc/rclone.conf
LOG=/var/log/aer-scrape.log
REASON=/var/log/aer-stop.json
: > "$LOG"
: > "$REASON"

# Ensure high NOFILE even under SSM non-interactive session
ulimit -n 1048576 || true

cd "$CODE_DIR"

# ===== Reliable upload helpers =====
# Upload a SNAPSHOT of a file (prevents checksum mismatches while a file is still being written)
upload_s3_snapshot() {{
  local src="$1" dst="$2"
  local snap; snap=$(mktemp /tmp/aerlog.XXXXXX) || return 1
  # Prefer cp; if copy fails for any reason, fall back to streaming copy
  if ! cp -f "$src" "$snap" 2>>"$LOG"; then
    cat "$src" >"$snap" 2>>"$LOG" || true
  fi
  local ok=1 tries=0
  while [ $tries -lt 6 ]; do
    tries=$((tries+1))
    if aws s3 cp "$snap" "$dst" --region "$REGION" >>"$LOG" 2>&1; then
      echo "$(date -Is) uploaded via aws: $src -> $dst" >>"$LOG"; ok=0; break
    fi
    local remote_dst="$REMOTE:$BUCKET/${{dst#s3://$BUCKET/}}"
    if command -v rclone >/dev/null 2>&1; then
      # copyto avoids directory scans; snapshot is stable so checksum will match
      if rclone copyto "$snap" "$remote_dst" >>"$LOG" 2>&1; then
        echo "$(date -Is) uploaded via rclone: $src -> $remote_dst" >>"$LOG"; ok=0; break
      fi
    fi
    sleep $((2 ** tries))
  done
  rm -f "$snap" || true
  return $ok
}}

# Identify instance
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
IID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || true)
NAME=$(curl -sfH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/Name 2>/dev/null || true)
if [ -z "$NAME" ] || [ "$NAME" = "Not found" ]; then
  NAME=$(aws ec2 describe-tags --region "$REGION" --filters Name=resource-id,Values="$IID" Name=key,Values=Name --query "Tags[0].Value" --output text 2>/dev/null || echo "unknown")
fi

# Decide PART from Name suffix
PART=""
case "$NAME" in *-??) PART="${{NAME##*-}}";; esac
if ! echo "$PART" | grep -Eq '^[0-9][0-9]$'; then PART=""; fi
[ -z "$PART" ] && P_IMDS=$(curl -sfH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/tags/instance/Part 2>/dev/null || true) && [ -n "$P_IMDS" ] && [ "$P_IMDS" != "Not found" ] && PART="$P_IMDS"
[ -z "$PART" ] && P_DESC=$(aws ec2 describe-tags --region "$REGION" --filters Name=resource-id,Values="$IID" Name=key,Values=Part --query "Tags[0].Value" --output text 2>/dev/null || true) && [ -n "$P_DESC" ] && [ "$P_DESC" != "None" ] && PART="$P_DESC"
[ -z "$PART" ] && [ -s /etc/aer/part ] && PART=$(cat /etc/aer/part || true)
[ -z "$PART" ] && PART="00"

# Paths
WELLS_FILE="wells_parts/wells_$PART.txt"
S3_LOG_URI="s3://$BUCKET/$S3_PREFIX/$NAME/$IID/aer-scrape.log"
S3_STOP_URI="s3://$BUCKET/$S3_PREFIX/$NAME/$IID/stop_reason.json"

# Header
{{
  echo "==== AER SCRAPER START ===="
  date -Is
  echo "Name:             $NAME"
  echo "InstanceId:       $IID"
  echo "Region:           $REGION"
  echo "Part:             $PART"
  echo "Wells file:       $WELLS_FILE"
  echo "Dashboards:       $DASH"
  echo "Out base:         $OUT_BASE"
  echo "S3 bucket/prefix: s3://$BUCKET/$PREFIX"
  echo "S3 log URI:       $S3_LOG_URI"
  echo "S3 stop JSON:     $S3_STOP_URI"
  echo "rclone remote:    $REMOTE"
  echo "Shutdown mode:    $SHUT_MODE"
  echo "ulimit -n:        $(ulimit -n 2>/dev/null || echo unknown)"
  aws --version 2>&1 || true
  aws sts get-caller-identity --output json 2>>"$LOG" || true
  command -v google-chrome >/dev/null && google-chrome --version || true
  command -v chromedriver   >/dev/null && chromedriver --version   || true
  python3.11 - <<'PY'
import platform
try:
  import selenium
  sel = selenium.__version__
except Exception:
  sel = '(not installed?)'
print("Python: " + platform.python_version() + "  Selenium: " + sel)
PY
  rclone version 2>/dev/null || true
  echo "==== ENV END ===="
}} | tee -a "$LOG"

# Mark run start & helpers
START_MARK="/tmp/aer_run_start.$$"
date -Is > "$START_MARK"

count_new_wells() {{
  local base="$1" mark="$2" c=0
  shopt -s nullglob
  for d in "$base"/*; do
    [ -d "$d" ] || continue
    if find "$d" -type f -newer "$mark" -print -quit | grep -q .; then c=$((c+1)); fi
  done
  echo "$c"
}}
count_total_wells() {{ find "$1" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | awk '{{print $1}}'; }}

# Defaults used by EXIT trap
NEW_WELLS=0
TOTAL_WELLS=0
STOP_REASON="normal-exit"
RC=0

# Write stop reason (runs on EXIT)
write_stop_reason() {{
  local rc_val="${{RC:-$?}}"
  local ts="$(date -Is 2>/dev/null || true)"
  cat > "$REASON" <<JSON || true
{{
  "timestamp": "${{ts}}",
  "instance_id": "${{IID}}",
  "name": "${{NAME}}",
  "part": "${{PART}}",
  "exit_code": ${{rc_val:-0}},
  "reason": "${{STOP_REASON}}",
  "shutdown_mode": "${{SHUT_MODE}}",
  "new_wells": ${{NEW_WELLS:-0}},
  "total_wells": ${{TOTAL_WELLS:-0}}
}}
JSON
  upload_s3_snapshot "$REASON" "$S3_STOP_URI" || true
}}
on_term() {{ STOP_REASON="signal:TERM"; echo "$(date -Is) got SIGTERM" | tee -a "$LOG"; }}
on_int()  {{ STOP_REASON="signal:INT";  echo "$(date -Is) got SIGINT"  | tee -a "$LOG"; }}
on_hup()  {{ STOP_REASON="signal:HUP";  echo "$(date -Is) got SIGHUP"  | tee -a "$LOG"; }}
trap write_stop_reason EXIT
trap on_term TERM
trap on_int  INT
trap on_hup  HUP

# Periodic log upload (snapshot each time)
upload_loop() {{
  while kill -0 "$1" 2>/dev/null; do
    upload_s3_snapshot "$LOG" "$S3_LOG_URI" || true
    sleep 300
  done
  upload_s3_snapshot "$LOG" "$S3_LOG_URI" || true
}}

# Run scraper
set +e
python3.11 scrape_and_push.py "$WELLS_FILE" \
  --bucket "$BUCKET" --remote "$REMOTE" --prefix "$PREFIX" \
  --out-base "$OUT_BASE" --workers 2 --dashboards "$DASH" --headless \
  --manifest-retries 3 --retry-wait 6 \
  2>&1 | tee -a "$LOG" &

SPID=$!
upload_loop "$SPID" &
UPID=$!
wait "$SPID"; RC=$?

# Post-run counts
NEW_WELLS=$(count_new_wells "$OUT_BASE" "$START_MARK")
TOTAL_WELLS=$(count_total_wells "$OUT_BASE" || echo 0)
{{
  echo "==== AER SCRAPER SUMMARY ===="
  date -Is
  echo "New wells this run:   $NEW_WELLS"
  echo "Total wells on disk:  $TOTAL_WELLS"
  echo "Scraper exit code:    $RC"
  echo "============================="
}} | tee -a "$LOG"

[ "$RC" -ne 0 ] && STOP_REASON="nonzero-exit" || true
wait "$UPID" || true

# Final best-effort uploads (snapshot to avoid checksum mismatch)
upload_s3_snapshot "$LOG" "$S3_LOG_URI" || true
upload_s3_snapshot "$REASON" "$S3_STOP_URI" || true

# Optional self-shutdown
if [ "$SHUT_MODE" = "stop" ] || [ "$SHUT_MODE" = "terminate" ]; then
  upload_s3_snapshot "$LOG" "$S3_LOG_URI" || true
  upload_s3_snapshot "$REASON" "$S3_STOP_URI" || true
  if [ "$SHUT_MODE" = "stop" ]; then
    # For EBS-backed Linux, a clean poweroff transitions instance to Stopped
    systemctl poweroff || shutdown -h now || true
  else
    # Termination still uses AWS API; if CLI is broken, instance will just power off
    aws ec2 terminate-instances --instance-ids "$IID" --region "$REGION" >>"$LOG" 2>&1 || true
  fi
fi

exit $RC
"""

def _fmt_wrapper(shutdown: str):
    return WRAPPER.format(
        region=REGION, bucket=BUCKET, s3_prefix=S3_LOG_PREFIX,
        code_dir=CODE_DIR, out_base=OUT_BASE, remote=REMOTE_NAME,
        prefix=PREFIX, dash=DASHBOARDS, shutdown=shutdown
    )

def _list_targets(all_flag: bool, ids: List[str], names: List[str], parts: List[str]) -> List[str]:
    if all_flag:
        filters = [{"Name": "tag:Role", "Values": [ROLE_TAG]}]
    else:
        filters = []
        if names:
            filters.append({"Name": "tag:Name", "Values": names})
        if parts:
            parts2 = [f"{int(p):02d}" for p in parts]
            filters.append({"Name": "tag:Part", "Values": parts2})
    if filters:
        resp = ec2.describe_instances(Filters=filters)
        out = []
        for r in resp["Reservations"]:
            for i in r["Instances"]:
                if i["State"]["Name"] in ("running", "pending"):
                    out.append(i["InstanceId"])
        return out
    return ids or []

def _wait_ssm_online(iid: str):
    while True:
        info = ssm.describe_instance_information()["InstanceInformationList"]
        ok = any(i["InstanceId"] == iid and i["PingStatus"] == "Online" for i in info)
        if ok: return
        time.sleep(8)

def _send_start(iid: str, shutdown_mode: str) -> str:
    script = _fmt_wrapper(shutdown_mode)
    params = {
        "commands": [
            "cat >/tmp/aer-run.sh <<'EOF'\n"+script+"\nEOF",
            "chmod +x /tmp/aer-run.sh",
            "bash /tmp/aer-run.sh"
        ]
    }
    resp = ssm.send_command(
        DocumentName="AWS-RunShellScript",
        InstanceIds=[iid],
        Parameters=params,
        TimeoutSeconds=60*60*6,
        CloudWatchOutputConfig={"CloudWatchOutputEnabled": False}
    )
    return resp["Command"]["CommandId"]

def _poll_and_shutdown(mapping: Dict[str, str], stop: bool, terminate: bool):
    remaining = set(mapping.keys())
    last_status: Dict[str, str] = {}
    print("Polling command status...")
    while remaining:
        done_now = []
        for iid in list(remaining):
            cmd = mapping[iid]
            try:
                inv = ssm.get_command_invocation(CommandId=cmd, InstanceId=iid)
                st = inv.get("Status", "Unknown")
            except botocore.exceptions.ClientError:
                st = "Pending"
            if last_status.get(iid) != st:
                print(f"{iid} → {st}")
                last_status[iid] = st
            if st not in ("Pending","InProgress","Delayed"):
                done_now.append(iid)
        for iid in done_now:
            remaining.discard(iid)
            if terminate:
                ec2.terminate_instances(InstanceIds=[iid])
                print(f"terminate sent → {iid}")
            elif stop:
                ec2.stop_instances(InstanceIds=[iid])
                print(f"stop sent → {iid}")
        time.sleep(15)
    print("All runs finished.")

def main():
    ap = argparse.ArgumentParser(
        description="Start scraping on targets; optional self-shutdown on instance; reliable S3 log uploads + stop-reason JSON."
    )
    tgt = ap.add_mutually_exclusive_group(required=True)
    tgt.add_argument("--all", action="store_true", help=f"target all instances with tag Role={ROLE_TAG}")
    tgt.add_argument("--ids", nargs="+", help="instance-ids")
    tgt.add_argument("--names", nargs="+", help="Name tags")
    tgt.add_argument("--parts", nargs="+", help="Part numbers, e.g., 00 01 02")

    act = ap.add_mutually_exclusive_group(required=True)
    act.add_argument("--stop", action="store_true", help="controller stops instances when run finishes")
    act.add_argument("--terminate", action="store_true", help="controller terminates instances when run finishes")
    act.add_argument("--self-stop", action="store_true", help="instance stops itself at end (controller can go offline)")
    act.add_argument("--self-terminate", action="store_true", help="instance terminates itself at end (controller can go offline)")

    args = ap.parse_args()

    targets = _list_targets(args.all, args.ids or [], args.names or [], args.parts or [])
    if not targets:
        print("No targets found."); return

    for iid in targets:
        _wait_ssm_online(iid)

    if args.self_stop:
        shutdown_mode = "stop"; controller_mode = None
    elif args.self_terminate:
        shutdown_mode = "terminate"; controller_mode = None
    elif args.stop:
        shutdown_mode = ""; controller_mode = "stop"
    else:
        shutdown_mode = ""; controller_mode = "terminate"

    mapping = {}
    for iid in targets:
        cmd_id = _send_start(iid, shutdown_mode)
        mapping[iid] = cmd_id
        print(json.dumps({"instance": iid, "command_id": cmd_id}))

    if controller_mode is None:
        print("Commands dispatched with self-shutdown. You can close your laptop.")
        return

    _poll_and_shutdown(mapping, stop=(controller_mode=="stop"), terminate=(controller_mode=="terminate"))

if __name__ == "__main__":
    main()
