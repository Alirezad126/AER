# modules/stop_scraping.py
import argparse, boto3, json
from .config import REGION, ROLE_TAG

ec2 = boto3.client("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

STOP_CMD = r"""bash -lc 'set -euo pipefail;
PID=$(pgrep -f -n "python3\.11 .*scrape_and_push\.py" || pgrep -f -n "python3\.11 .*scraping\.py" || true);
if [ -n "$PID" ]; then
  PGID=$(ps -o pgid= -p "$PID" | tr -d " ");
  echo "Stopping PGID=$PGID (PID=$PID)";
  kill -TERM -$PGID || true;
  pkill -TERM -f "scraping\.py" || true;
  pkill -TERM -f chromedriver || true;
  pkill -TERM -f google-chrome || true;
  sleep 5;
  kill -KILL -$PGID || true;
  pkill -KILL -f "scraping\.py" || true;
  pkill -KILL -f chromedriver || true;
  pkill -KILL -f google-chrome || true;
else
  echo "No scraper parent found";
fi;
ps -eo pid,ppid,pgid,user,cmd | egrep -E "(scrape_and_push\.py|scraping\.py|chromedriver|google-chrome)" | grep -v egrep || echo "None"; exit 0'"""

def find_by_role():
    ec2c = ec2.describe_instances(Filters=[{"Name":"tag:Role","Values":[ROLE_TAG]}])
    out=[]
    for r in ec2c["Reservations"]:
        for i in r["Instances"]:
            if i["State"]["Name"] in ("running","pending"):
                out.append(i["InstanceId"])
    return out

def main():
    ap = argparse.ArgumentParser(description="Stop scraper processes via SSM.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help=f"target all instances tagged Role={ROLE_TAG}")
    g.add_argument("--ids", nargs="+", help="specific instance-ids")
    args = ap.parse_args()

    targets = find_by_role() if args.all else args.ids
    if not targets:
        print("no targets"); return

    for iid in targets:
        resp = ssm.send_command(
            DocumentName="AWS-RunShellScript",
            InstanceIds=[iid],
            Parameters={"commands":[STOP_CMD]},
            TimeoutSeconds=600
        )
        print(json.dumps({"instance": iid, "command_id": resp["Command"]["CommandId"]}))

if __name__ == "__main__":
    main()
