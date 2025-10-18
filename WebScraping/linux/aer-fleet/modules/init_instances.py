# modules/init_instances.py
import argparse, base64, time, boto3, json
from pathlib import Path
from .config import REGION, ROLE_TAG, NAME_PREFIX, LAUNCH_TEMPLATE_ID, USER_DATA_FILE

ec2 = boto3.client("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

def launch_one(part: int) -> str:
    name = f"{NAME_PREFIX}-{part:02d}"
    ud = Path(USER_DATA_FILE).read_text()
    resp = ec2.run_instances(
        LaunchTemplate={"LaunchTemplateId": LAUNCH_TEMPLATE_ID, "Version": "$Latest"},
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": name},
                {"Key": "Role", "Value": ROLE_TAG},
                {"Key": "Part", "Value": f"{part:02d}"},
            ],
        }],
        UserData=ud,  # EC2 expects plain text; CLI auto-encodes; boto3 does too
        MinCount=1, MaxCount=1,
    )
    iid = resp["Instances"][0]["InstanceId"]
    print(f"launched {name} => {iid}")
    return iid

def wait_instance_ok(instance_ids):
    print("waiting for instance-status-ok ...")
    waiter = ec2.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=instance_ids)

def wait_ssm_online(instance_id):
    print(f"waiting for SSM Online: {instance_id}")
    while True:
        info = ssm.describe_instance_information()["InstanceInformationList"]
        online = any(i["InstanceId"] == instance_id and i["PingStatus"] == "Online" for i in info)
        if online: break
        time.sleep(8)

def main():
    ap = argparse.ArgumentParser(description="Launch N instances from Launch Template and wait for SSM Online.")
    ap.add_argument("--count", type=int, required=True, help="Number of instances")
    ap.add_argument("--start-part", type=int, default=0, help="Starting part index (default 0)")
    args = ap.parse_args()

    ids = [launch_one(args.start_part + i) for i in range(args.count)]
    wait_instance_ok(ids)
    for iid in ids:
        wait_ssm_online(iid)

    print(json.dumps({"instances": ids}, indent=2))

if __name__ == "__main__":
    main()
