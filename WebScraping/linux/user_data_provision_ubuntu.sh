#!/bin/bash
set -euxo pipefail

# ====== EDIT THESE IF NEEDED ======
REGION="us-east-1"
BUCKET="aer-scrape-prod"
REPO_URL="https://github.com/Alirezad126/AER.git"
CODE_SUBDIR="WebScraping/linux"         # path inside repo
REPO_DIR="/opt/aer-scraper"             # where we clone the repo
OUT_BASE="/data/Data"                   # where your script writes locally
PART_FILE="/etc/aer/part"               # where we store the Part tag
RCLONE_CFG="/etc/rclone.conf"           # global rclone config file
REMOTE_NAME="s3aer"                     # rclone remote name
# ==================================

export DEBIAN_FRONTEND=noninteractive

# --- Base updates & tools ---
apt-get update -y
apt-get upgrade -y
apt-get install -y \
  ca-certificates curl gnupg lsb-release apt-transport-https software-properties-common \
  git unzip jq tar gzip \
  fonts-dejavu-core fonts-liberation \
  awscli

# --- SSM Agent (Ubuntu) ---
snap install amazon-ssm-agent --classic || true
systemctl enable snap.amazon-ssm-agent.amazon-ssm-agent || true
systemctl start  snap.amazon-ssm-agent.amazon-ssm-agent || true

# --- Google Chrome (APT repo) ---
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg
chmod a+r /etc/apt/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
  > /etc/apt/sources.list.d/google-chrome.list
apt-get update -y
apt-get install -y google-chrome-stable

# --- Python 3.11 + pip + Selenium (Selenium Manager will fetch chromedriver) ---
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -y
apt-get install -y python3.11 python3.11-distutils python3.11-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
python3.11 -m pip install --upgrade pip
python3.11 -m pip install "selenium>=4.10"

# --- rclone (install) ---
curl -fsSL https://rclone.org/install.sh | bash

# --- rclone (GLOBAL CONFIG at /etc/rclone.conf) ---
cat > "${RCLONE_CFG}" <<EOF
[${REMOTE_NAME}]
type = s3
provider = AWS
env_auth = true
region = ${REGION}
EOF
chmod 0644 "${RCLONE_CFG}"

# Make sure all shells/services see it by default
echo "RCLONE_CONFIG=${RCLONE_CFG}" >> /etc/environment
cat > /etc/profile.d/rclone.sh <<EOF
export RCLONE_CONFIG=${RCLONE_CFG}
EOF
chmod 0644 /etc/profile.d/rclone.sh

# --- Get your code ---
mkdir -p "${REPO_DIR}"
if [ ! -d "${REPO_DIR}/.git" ]; then
  git clone --depth=1 "${REPO_URL}" "${REPO_DIR}"
else
  (cd "${REPO_DIR}" && git pull --ff-only) || true
fi

# --- Pull wells_parts from S3 ---
mkdir -p "${REPO_DIR}/${CODE_SUBDIR}/wells_parts"
RCLONE_CONFIG="${RCLONE_CFG}" rclone copy "${REMOTE_NAME}:${BUCKET}/wells_parts" "${REPO_DIR}/${CODE_SUBDIR}/wells_parts" || true

# --- Record our Part tag (from instance tag "Part") ---
mkdir -p /etc/aer
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
IID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || true)
aws ec2 describe-tags \
  --region "${REGION}" \
  --filters "Name=resource-id,Values=${IID}" "Name=key,Values=Part" \
  --query "Tags[0].Value" --output text > "${PART_FILE}" || echo "00" > "${PART_FILE}"
chmod 0644 "${PART_FILE}"

# --- Prepare output folder ---
mkdir -p "${OUT_BASE}"

echo "Provisioning complete. Ready for SSM trigger."
