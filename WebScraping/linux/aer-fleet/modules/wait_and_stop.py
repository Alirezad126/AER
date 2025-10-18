# modules/wait_and_stop.py
import argparse, time, boto3
from .config import REGION

ssm = boto3.client("ssm", region_name=REGION)
ec2 = boto3.client("ec2", region_name=REGION)

def main():
    ap = argparse.ArgumentParser(description="Wait for SSM command completion, then stop instance.")
    ap.add_argument("--instance-id", required=True)
    ap.add_argument("--command-id", required=True)
    ap.add_argument("--stop", action="store_true", help="stop instance on completion")
    ap.add_argument("--terminate", action="store_true", help="terminate instead of stop")
    args = ap.parse_args()

    while True:
        try:
            resp = ssm.get_command_invocation(
                CommandId=args.command_id, InstanceId=args.instance_id
            )
            st = resp["Status"]
            print(f"Status: {st}")
            if st not in ("Pending","InProgress","Delayed"):
                print("ExitCode:", resp.get("ResponseCode"))
                break
        except ssm.exceptions.InvocationDoesNotExist:
            pass
        time.sleep(15)

    if args.terminate:
        ec2.terminate_instances(InstanceIds=[args.instance_id])
        print("Terminate sent.")
    elif args.stop:
        ec2.stop_instances(InstanceIds=[args.instance_id])
        print("Stop sent.")

if __name__ == "__main__":
    main()
