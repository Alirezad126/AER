#!/usr/bin/env python3
# Launch N instances with separated egress IPs.
# Modes:
#   --ip-mode eip      -> allocate+attach one Elastic IP per instance (guaranteed unique IP)
#   --ip-mode public   -> launch in provided public subnets (round-robin) that map public IPs
#   --ip-mode none     -> do not enforce uniqueness (use LT's networking as-is)
#
# If your Launch Template (LT) already defines NetworkInterfaces, we cannot override
# subnet/public-IP at run time; you must embed those choices in the LT or use EIPs.

import argparse, base64, time, json, boto3, botocore, sys
from pathlib import Path
from typing import List, Optional

from .config import (
    REGION, ROLE_TAG, NAME_PREFIX, LAUNCH_TEMPLATE_ID, USER_DATA_FILE,
)

ec2 = boto3.client("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
iam = boto3.client("iam")
ltc = boto3.client("ec2", region_name=REGION)  # for LT describe convenience

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

def _get_latest_lt_data(launch_template_id: str) -> dict:
    v = ltc.describe_launch_template_versions(
        LaunchTemplateId=launch_template_id,
        Versions=["$Latest"]
    )["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    return v

def _lt_defines_nics(lt_data: dict) -> bool:
    nis = lt_data.get("NetworkInterfaces") or []
    return len(nis) > 0

def _get_profile_role_from_lt(launch_template_id: str) -> Optional[str]:
    v = _get_latest_lt_data(launch_template_id)
    prof = v.get("IamInstanceProfile") or {}
    name = prof.get("Name")
    arn  = prof.get("Arn")
    if not name and not arn:
        return None
    if arn and not name:
        name = arn.split("/")[-1]
    ip = iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]
    roles = ip.get("Roles") or []
    if not roles:
        return None
    return roles[0]["RoleName"]

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

def _subnet_maps_public_ip(subnet_id: str) -> Optional[bool]:
    try:
        resp = ec2.describe_subnets(SubnetIds=[subnet_id])
        sub = resp["Subnets"][0]
        # Newer accounts: 'MapPublicIpOnLaunch' attribute must be fetched separately
        attr = ec2.describe_subnet_attribute(
            SubnetId=subnet_id, MapPublicIpOnLaunch={}
        )
        return bool(attr["MapPublicIpOnLaunch"]["Value"])
    except botocore.exceptions.ClientError as e:
        print(f"[warn] could not query MapPublicIpOnLaunch for {subnet_id}: {e}")
        return None

def _allocate_and_attach_eip(iid: str, name_tag: str):
    alloc = ec2.allocate_address(Domain="vpc")
    ec2.associate_address(InstanceId=iid, AllocationId=alloc["AllocationId"])
    # tag the EIP allocation for bookkeeping
    try:
        ec2.create_tags(Resources=[alloc["AllocationId"]],
                        Tags=[{"Key":"Name","Value":name_tag},
                              {"Key":"Role","Value":ROLE_TAG}])
    except Exception:
        pass
    print(f"[ok] EIP {alloc['PublicIp']} associated → {iid} ({name_tag})")

def launch_one(part: int,
               enable_imds_tags: bool,
               ip_mode: str,
               subnet_rr: Optional[List[str]],
               lt_has_nics: bool) -> str:
    """
    ip_mode: 'eip' | 'public' | 'none'
    subnet_rr: optional list of public subnet ids; when provided we round-robin (for 'public' or even 'none' if LT doesn't define NICs)
    """
    assert ip_mode in ("eip", "public", "none")
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

    # --- Choose subnet for this instance if possible ---
    chosen_subnet = None
    if subnet_rr and not lt_has_nics:
        # We can provide SubnetId only if LT does not define NICs
        idx = part % len(subnet_rr)
        chosen_subnet = subnet_rr[idx]
        req["SubnetId"] = chosen_subnet

    # NOTE:
    # If LT defines NetworkInterfaces, we cannot set SubnetId here (AWS error).
    # Ensure your LT's NIC uses a public subnet (and AssociatePublicIpAddress=true) if you want public IPs,
    # or use ip_mode='eip' to attach Elastic IPs post-launch regardless of subnet.

    resp = ec2.run_instances(**req)
    iid = resp["Instances"][0]["InstanceId"]
    print(f"launched {name} => {iid}{' in '+chosen_subnet if chosen_subnet else ''}")

    # --- Post-launch egress setup ---
    if ip_mode == "public":
        if lt_has_nics:
            print("[warn] LT defines NetworkInterfaces; cannot enforce public IP at run time. "
                  "Make sure the LT NIC is in a public subnet with AssociatePublicIpAddress enabled.")
        else:
            # We relied on the subnet's MapPublicIpOnLaunch setting.
            if chosen_subnet:
                flag = _subnet_maps_public_ip(chosen_subnet)
                if flag is False:
                    print(f"[warn] Subnet {chosen_subnet} does NOT map public IP on launch. "
                          f"Enable it or use --ip-mode eip for guaranteed unique IPs.")
    elif ip_mode == "eip":
        # EIP works even in private subnets (outbound via NAT will still exist), but for direct egress,
        # you normally place instances in a public subnet with IGW and a security group allowing outbound 443.
        _allocate_and_attach_eip(iid, name)

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
        if online:
            return
        time.sleep(8)

def main():
    ap = argparse.ArgumentParser(
        description="Launch N instances from Launch Template with separated egress (EIP or public subnets)."
    )
    ap.add_argument("--count", type=int, required=True, help="How many instances to launch")
    ap.add_argument("--start-part", type=int, default=0, help="Starting part index (default 0)")
    ap.add_argument("--enable-imds-tags", action="store_true", help="Enable InstanceMetadataTags=enabled")
    ap.add_argument("--ensure-self-shutdown-policy", action="store_true",
                    help="Attach inline policy on the LT role to allow stop/terminate self")

    ap.add_argument("--ip-mode", choices=["eip","public","none"], default="eip",
                    help="How to ensure unique egress IPs: 'eip' (recommended), 'public', or 'none'.")

    ap.add_argument("--subnets", nargs="+",
                    help="List of PUBLIC subnet-ids (round-robin). Used by --ip-mode public (or none when LT has no NICs).")

    args = ap.parse_args()

    # Check LT NIC situation once
    lt_data = _get_latest_lt_data(LAUNCH_TEMPLATE_ID)
    lt_has_nics = _lt_defines_nics(lt_data)
    if lt_has_nics and args.subnets:
        print("[info] Launch Template defines NetworkInterfaces; cannot override subnet at run time. "
              "Provided --subnets will be ignored.")

    if args.ensure_self_shutdown_policy:
        role_name = _get_profile_role_from_lt(LAUNCH_TEMPLATE_ID)
        if role_name:
            _ensure_self_shutdown_policy(role_name)
        else:
            print("[warn] could not determine role from Launch Template; skipping policy attach")

    # Launch
    ids = []
    for i in range(args.count):
        part = args.start_part + i
        iid = launch_one(
            part=part,
            enable_imds_tags=args.enable_imds_tags,
            ip_mode=args.ip_mode,
            subnet_rr=args.subnets,
            lt_has_nics=lt_has_nics
        )
        ids.append(iid)

    # Health + SSM
    wait_instance_ok(ids)
    for iid in ids:
        wait_ssm_online(iid)

    print(json.dumps({"instances": ids}, indent=2))

if __name__ == "__main__":
    main()
