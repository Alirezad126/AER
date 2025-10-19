# modules/init_instances.py
import argparse, base64, time, json, boto3, botocore
from pathlib import Path
from typing import List, Optional, Tuple
from .config import (
    REGION, ROLE_TAG, NAME_PREFIX, LAUNCH_TEMPLATE_ID, USER_DATA_FILE,
)

ec2 = boto3.client("ec2", region_name=REGION)
iam = boto3.client("iam")
lt  = boto3.client("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

SELF_SHUTDOWN_POLICY_NAME = "AERSelfShutdown"
SELF_SHUTDOWN_POLICY_DOC = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "SelfStopOrTerminate",
        "Effect": "Allow",
        "Action": ["ec2:StopInstances", "ec2:TerminateInstances", "ec2:DescribeInstances"],
        "Resource": "*"
    }]
})

def _lt_network_defaults(launch_template_id: str) -> Tuple[List[str], Optional[str]]:
    """Return (security_group_ids, subnet_id_from_lt_or_None)"""
    v = lt.describe_launch_template_versions(
        LaunchTemplateId=launch_template_id, Versions=["$Latest"]
    )["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    # SGs may be defined either in NetworkInterfaces or as SecurityGroupIds
    sgs = []
    if "NetworkInterfaces" in v and v["NetworkInterfaces"]:
        for ni in v["NetworkInterfaces"]:
            sgs += ni.get("Groups", [])
    sgs = sgs or v.get("SecurityGroupIds", [])
    subnet_id = None
    if "NetworkInterfaces" in v and v["NetworkInterfaces"]:
        subnet_id = v["NetworkInterfaces"][0].get("SubnetId")
    else:
        subnet_id = v.get("SubnetId")
    return (sgs, subnet_id)

def _get_profile_role_from_lt(launch_template_id: str) -> Optional[str]:
    v = lt.describe_launch_template_versions(
        LaunchTemplateId=launch_template_id, Versions=["$Latest"]
    )["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    prof = v.get("IamInstanceProfile") or {}
    name = prof.get("Name")
    arn  = prof.get("Arn")
    if not name and not arn:
        return None
    if arn and not name:
        name = arn.split("/")[-1]
    ip = iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]
    roles = ip.get("Roles") or []
    return roles[0]["RoleName"] if roles else None

def _ensure_self_shutdown_policy(role_name: str):
    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName=SELF_SHUTDOWN_POLICY_NAME,
            PolicyDocument=SELF_SHUTDOWN_POLICY_DOC
        )
        print(f"[ok] ensured inline policy on role {role_name}: {SELF_SHUTDOWN_POLICY_NAME}")
    except botocore.exceptions.ClientError as e:
        print(f"[warn] could not attach inline policy to {role_name}: {e}")

def launch_one(part: int, enable_imds_tags: bool, ip_mode: str,
               subnets: List[str], cycle_idx: int) -> str:
    """
    ip_mode:
      - 'public'  : auto-assigned public IPv4 per instance (no EIP).
      - 'none'    : use whatever the Launch Template defines.
      - 'eip'     : (not used here due to your 5-EIP limit).
    """
    name = f"{NAME_PREFIX}-{part:02d}"
    ud_txt = Path(USER_DATA_FILE).read_text()
    ud_b64 = base64.b64encode(ud_txt.encode("utf-8")).decode("ascii")

    req = {
        "LaunchTemplate": {"LaunchTemplateId": LAUNCH_TEMPLATE_ID, "Version": "$Latest"},
        "TagSpecifications": [{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": name},
                {"Key": "Role", "Value": ROLE_TAG},
                {"Key": "Part", "Value": f"{part:02d}"},
            ],
        }],
        "UserData": ud_b64,
        "MinCount": 1, "MaxCount": 1,
        "MetadataOptions": {
            "HttpTokens": "required",
            "InstanceMetadataTags": "enabled" if enable_imds_tags else "disabled"
        },
    }

    if ip_mode == "public":
        # override NIC to ensure each instance has its own public IPv4
        sg_ids, _ = _lt_network_defaults(LAUNCH_TEMPLATE_ID)
        nic = {"DeviceIndex": 0, "AssociatePublicIpAddress": True}
        if subnets:
            nic["SubnetId"] = subnets[cycle_idx % len(subnets)]
        if sg_ids:
            nic["Groups"] = sg_ids
        req["NetworkInterfaces"] = [nic]
    elif ip_mode == "none":
        if subnets:
            # You may still want to spread across subnets even when not forcing public IP
            req["SubnetId"] = subnets[cycle_idx % len(subnets)]
    else:
        raise ValueError("Use --ip-mode public (no EIP) or --ip-mode none. (EIP not used due to 5-EIP limit)")

    resp = ec2.run_instances(**req)
    iid = resp["Instances"][0]["InstanceId"]
    print(f"launched {name} => {iid} (ip_mode={ip_mode})")
    return iid

def wait_instance_ok(instance_ids: List[str]):
    print("waiting for instance-status-ok ...")
    waiter = ec2.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=instance_ids)

def wait_ssm_online(instance_id: str):
    print(f"waiting for SSM Online: {instance_id}")
    while True:
        info = ssm.describe_instance_information()["InstanceInformationList"]
        online = any(i["InstanceId"] == instance_id and i["PingStatus"] == "Online" for i in info)
        if online: return
        time.sleep(8)

def main():
    ap = argparse.ArgumentParser(
        description="Launch N instances from Launch Template, tag with Part, give each its own public IPv4, and wait for SSM."
    )
    ap.add_argument("--count", type=int, required=True, help="How many instances to launch")
    ap.add_argument("--start-part", type=int, default=0, help="Starting Part index (default 0)")
    ap.add_argument("--enable-imds-tags", action="store_true", help="Enable InstanceMetadataTags=enabled")
    ap.add_argument("--ensure-self-shutdown-policy", action="store_true",
                    help="Attach inline policy on the LT role to allow stop/terminate self")
    ap.add_argument("--ip-mode", choices=["public","none"], default="public",
                    help="public = one public IPv4 per instance (no EIP). none = LT default.")
    ap.add_argument("--subnet-ids", nargs="*", default=[],
                    help="Optional list of subnet-ids to round-robin across")

    args = ap.parse_args()

    if args.ensure_self_shutdown_policy:
        role_name = _get_profile_role_from_lt(LAUNCH_TEMPLATE_ID)
        if role_name:
            _ensure_self_shutdown_policy(role_name)
        else:
            print("[warn] could not determine role from Launch Template; skipping policy attach")

    ids = []
    for i in range(args.count):
        part = args.start_part + i
        iid = launch_one(part, args.enable_imds_tags, args.ip_mode, args.subnet_ids, i)
        ids.append(iid)

    wait_instance_ok(ids)
    for iid in ids:
        wait_ssm_online(iid)

    print(json.dumps({"instances": ids}, indent=2))

if __name__ == "__main__":
    main()
