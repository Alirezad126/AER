#!/bin/bash
set -euxo pipefail

REGION="us-east-1"
BUCKET="aer-scrape-prod"
REPO_URL="https://github.com/Alirezad126/AER.git"
CODE_SUBDIR="WebScraping/linux"
REPO_DIR="/opt/aer-scraper"
OUT_BASE="/data/Data"
PART_FILE="/etc/aer/part"
RCLONE_CFG="/etc/rclone.conf"
REMOTE_NAME="s3aer"

export DEBIAN_FRONTEND=noninteractive

# --- base packages ---
apt-get update -y
apt-get upgrade -y
apt-get install -y \
  ca-certificates curl gnupg lsb-release apt-transport-https software-properties-common \
  git unzip jq tar gzip \
  fonts-dejavu-core fonts-liberation \
  awscli

# --- 3 GB swap (uses root EBS space) ---
if ! swapon --show | grep -q '^/swapfile'; then
  fallocate -l 3G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=3072
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  if ! grep -q '^/swapfile ' /etc/fstab; then
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi
  # keep RAM preferred over swap
  echo -e 'vm.swappiness=20\nvm.vfs_cache_pressure=50' >/etc/sysctl.d/99-swap.conf
  sysctl --system || true
fi

# --- SSM Agent ---
snap install amazon-ssm-agent --classic || true
systemctl enable snap.amazon-ssm-agent.amazon-ssm-agent || true
systemctl start  snap.amazon-ssm-agent.amazon-ssm-agent || true

# --- Google Chrome ---
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg
chmod a+r /etc/apt/keyrings/google-chrome.gpg
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
  > /etc/apt/sources.list.d/google-chrome.list
apt-get update -y
apt-get install -y google-chrome-stable

# --- Python 3.11 + Selenium ---
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update -y
apt-get install -y python3.11 python3.11-distutils python3.11-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
python3.11 -m pip install --upgrade pip
python3.11 -m pip install "selenium>=4.10"

# --- rclone (env_auth to use instance role) ---
curl -fsSL https://rclone.org/install.sh | bash
cat > "${RCLONE_CFG}" <<EOF
[${REMOTE_NAME}]
type = s3
provider = AWS
env_auth = true
region = ${REGION}
EOF
chmod 0644 "${RCLONE_CFG}"
echo "RCLONE_CONFIG=${RCLONE_CFG}" >> /etc/environment
cat > /etc/profile.d/rclone.sh <<EOF
export RCLONE_CONFIG=${RCLONE_CFG}
EOF
chmod 0644 /etc/profile.d/rclone.sh

# --- code checkout / update ---
mkdir -p "${REPO_DIR}"
if [ ! -d "${REPO_DIR}/.git" ]; then
  git clone --depth=1 "${REPO_URL}" "${REPO_DIR}"
else
  (cd "${REPO_DIR}" && git pull --ff-only) || true
fi

# --- hydrate wells_parts from S3 if available ---
mkdir -p "${REPO_DIR}/${CODE_SUBDIR}/wells_parts"
RCLONE_CONFIG="${RCLONE_CFG}" rclone copy "${REMOTE_NAME}:${BUCKET}/wells_parts" "${REPO_DIR}/${CODE_SUBDIR}/wells_parts" || true

# --- record Part tag to /etc/aer/part ---
mkdir -p /etc/aer
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)
IID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id || true)
aws ec2 describe-tags --region "${REGION}" \
  --filters "Name=resource-id,Values=${IID}" "Name=key,Values=Part" \
  --query "Tags[0].Value" --output text > "${PART_FILE}" || echo "00" > "${PART_FILE}"
chmod 0644 "${PART_FILE}"

# --- data directory ---
mkdir -p "${OUT_BASE}"

echo "Provisioning complete. Ready for SSM trigger."
