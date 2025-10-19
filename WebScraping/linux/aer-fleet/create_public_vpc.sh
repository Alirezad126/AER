#!/usr/bin/env bash
set -euo pipefail

# ---- settings (change Name if you want a different stack label) ----
STACK="aer-scrape"
CIDR_VPC="10.42.0.0/16"
CIDR_SUBNET_A="10.42.0.0/20"
CIDR_SUBNET_B="10.42.16.0/20"
REGION="${AWS_REGION:-us-east-1}"

tag() { printf 'ResourceType=%s,Tags=[{Key=Name,Value=%s-%s}]' "$1" "$STACK" "$2"; }

# ---- 1) VPC (+ DNS on) ----
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block "$CIDR_VPC" \
  --tag-specifications "$(tag vpc vpc)" \
  --query 'Vpc.VpcId' --output text)
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-support

# ---- 2) Internet Gateway + attach ----
IGW_ID=$(aws ec2 create-internet-gateway \
  --tag-specifications "$(tag internet-gateway igw)" \
  --query 'InternetGateway.InternetGatewayId' --output text)
aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"

# ---- 3) Pick two AZs ----
AZS=($(aws ec2 describe-availability-zones --query 'AvailabilityZones[].ZoneName' --output text | tr '\t' '\n' | head -n 2))
AZA="${AZS[0]}"; AZB="${AZS[1]}"

# ---- 4) Two public subnets + auto-assign public IPv4 ----
SUBNET_A=$(aws ec2 create-subnet \
  --vpc-id "$VPC_ID" --availability-zone "$AZA" --cidr-block "$CIDR_SUBNET_A" \
  --tag-specifications "$(tag subnet public-a)" \
  --query 'Subnet.SubnetId' --output text)
SUBNET_B=$(aws ec2 create-subnet \
  --vpc-id "$VPC_ID" --availability-zone "$AZB" --cidr-block "$CIDR_SUBNET_B" \
  --tag-specifications "$(tag subnet public-b)" \
  --query 'Subnet.SubnetId' --output text)
aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_A" --map-public-ip-on-launch
aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_B" --map-public-ip-on-launch

# ---- 5) Route table with 0.0.0.0/0 -> IGW, associate to both subnets ----
RTB_ID=$(aws ec2 create-route-table \
  --vpc-id "$VPC_ID" \
  --tag-specifications "$(tag route-table public)" \
  --query 'RouteTable.RouteTableId' --output text)
aws ec2 create-route --route-table-id "$RTB_ID" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID"
aws ec2 associate-route-table --subnet-id "$SUBNET_A" --route-table-id "$RTB_ID" >/dev/null
aws ec2 associate-route-table --subnet-id "$SUBNET_B" --route-table-id "$RTB_ID" >/dev/null

# ---- 6) Security group (all egress; optional SSH ingress) ----
SG_ID=$(aws ec2 create-security-group \
  --group-name "${STACK}-sg" --description "AER scraping egress" --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)
# allow all outbound
aws ec2 authorize-security-group-egress --group-id "$SG_ID" \
  --ip-permissions IpProtocol=-1,IpRanges="[{CidrIp=0.0.0.0/0}]"
# optional: SSH from your current IP
MYIP=$(curl -s https://checkip.amazonaws.com || echo "")
if [[ -n "$MYIP" ]]; then
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
    --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=${MYIP%$'\r'}/32,Description=ssh-me}]"
fi

echo "Done."
echo "VPC_ID       = $VPC_ID"
echo "SUBNET_IDS   = $SUBNET_A $SUBNET_B"
echo "SECURITY_GRP = $SG_ID"
