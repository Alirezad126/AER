#!/bin/bash
set -euxo pipefail

# ===== CONFIGURE =====
REGION="us-east-1"                         # bucket region
BUCKET="aer-scrape-prod"
REMOTE_NAME="s3aer"                         # must match your code
PREFIX="Data"
PART_IDX="00"                               # test part
REPO_URL="https://github.com/<owner>/<repo>.git"
REPO_DIR="/opt/aer-scraper"
CODE_SUBDIR="WebScraping/linux"             # where your py files live
OUT_BASE="/data/Data"

# ===== SYSTEM PACKAGES =====
dnf -y update
dnf -y install chromium chromedriver python3.11 python3.11-pip git tar gzip unzip \
                 dejavu-sans-fonts liberation-fonts # fonts help headless Chrome render

# rclone (official installer)
curl -fsSL https://rclone.org/install.sh | bash

# ===== PYTHON DEPS =====
python3.11 -m pip install --upgrade pip
python3.11 -m pip install "selenium>=4.10"

# ===== RCLONE REMOTE USING IAM ROLE =====
rclone config create "${REMOTE_NAME}" s3 provider AWS env_auth true region "${REGION}" --non-interactive

# ===== GET CODE FROM PUBLIC GITHUB =====
mkdir -p "${REPO_DIR}"
git clone --depth=1 "${REPO_URL}" "${REPO_DIR}" || true

# ===== FETCH wells_parts FROM S3 =====
cd "${REPO_DIR}/${CODE_SUBDIR}"
mkdir -p wells_parts
rclone copy "${REMOTE_NAME}:${BUCKET}/wells_parts" wells_parts

# ===== RUN YOUR WRAPPER EXACTLY =====
mkdir -p "${OUT_BASE}"
PART_FILE="wells_parts/wells_${PART_IDX}.txt"

python3.11 scrape_and_push.py "${PART_FILE}" \
  --bucket "${BUCKET}" --remote "${REMOTE_NAME}" --prefix "${PREFIX}" \
  --out-base "${OUT_BASE}" --workers 1 --dashboards Reservoir_Evaluation --headless

# OPTIONAL: stop when done so you don't pay for idle time
shutdown -h +1 || true
