# modules/run_scraping.py
import argparse, boto3, json
from .config import (
    REGION, BUCKET, S3_LOG_PREFIX, ROLE_TAG, CODE_DIR, OUT_BASE,
    REMOTE_NAME, PREFIX, DASHBOARDS
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

export RCLONE_CONFIG=/etc/rclone.conf
LOG=/var/log/aer-scrape.log
: > "$LOG"

cd "$CODE_DIR"

# discover identifiers for log path
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
IID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || true)
NAME=$(aws ec2 describe-tags --region "$REGION" --filters Name=resource-id,Values=${IID} Name=key,Values=Name --query "Tags[0].Value" --output text 2>/dev/null || echo "unknown")
PART=$(cat /etc/aer/part 2>/dev/null || echo "00")

S3_LOG_URI="s3://${BUCKET}/${S3_PREFIX}/${NAME}/${IID}/aer-scrape.log"

# background uploader: every 300s push the current log; exit when main ends
upload_loop() {{
  while kill -0 "$1" 2>/dev/null; do
    aws s3 cp "$LOG" "$S3_LOG_URI" --region "$REGION" || true
    sleep 300
  done
  aws s3 cp "$LOG" "$S3_LOG_URI" --region "$REGION" || true
}}

# run scraper and tee output
set +e
python3.11 scrape_and_push.py "wells_parts/wells_${{PART}}.txt" \
  --bucket "${BUCKET}" --remote "${REMOTE}" --prefix "${PREFIX}" \
  --out-base "${OUT_BASE}" --workers 1 --dashboards "${DASH}" --headless \
  2>&1 | tee -a "$LOG" &
SPID=$!

upload_loop "$SPID" &
UPID=$!

wait "$SPID"; RC=$?
wait "$UPID" || true
exit $RC
"""

def find_instances(target_all: bool, ids: list, names: list, parts: list):
    if target_all:
        f = [{"Name":"tag:Role","Values":[ROLE_TAG]}]
    else:
        f = []
        if names:
            f.append({"Name":"tag:Name","Values":names})
        if parts:
            f.append({"Name":"tag:Part","Values":[f"{int(p):02d}" for p in parts]})
        if ids:
            f.append({"Name":"instance-id","Values":ids})
    resp = ec2.describe_instances(Filters=f) if f else ec2.describe_instances(InstanceIds=ids)
    out = []
    for r in resp["Reservations"]:
        for i in r["Instances"]:
            if i["State"]["Name"] in ("running","pending"):
                out.append(i["InstanceId"])
    return out

def send_run(iid: str):
    script = WRAPPER.format(
        region=REGION, bucket=BUCKET, s3_prefix=S3_LOG_PREFIX,
        code_dir=CODE_DIR, out_base=OUT_BASE, remote=REMOTE_NAME,
        prefix=PREFIX, dash=DASHBOARDS
    )
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
        TimeoutSeconds=60*60*4,  # 4h
        CloudWatchOutputConfig={"CloudWatchOutputEnabled": False}
    )
    cmd_id = resp["Command"]["CommandId"]
    print(json.dumps({"instance": iid, "command_id": cmd_id}))
    return cmd_id

def main():
    ap = argparse.ArgumentParser(description="Run scraping via SSM with 5-min S3 log uploads.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help=f"target all instances tagged Role={ROLE_TAG}")
    g.add_argument("--ids", nargs="+", help="specific instance-ids")
    g.add_argument("--names", nargs="+", help="specific Name tags")
    g.add_argument("--parts", nargs="+", help="target Part numbers, e.g. 00 01 02")

    args = ap.parse_args()
    targets = find_instances(args.all, args.ids or [], args.names or [], args.parts or [])
    if not targets:
        print("no targets"); return

    for iid in targets:
        send_run(iid)

if __name__ == "__main__":
    main()
