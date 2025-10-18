# modules/config.py
REGION = "us-east-1"
BUCKET = "aer-scrape-prod"
ROLE_TAG = "AERScraper"
NAME_PREFIX = "aer-prep"         # instances named like aer-prep-00, aer-prep-01, ...
LAUNCH_TEMPLATE_ID = "lt-047287b2914fb1b35"  # <-- set yours

USER_DATA_FILE = "user_data_provision_ubuntu.sh"

# Where the wrapper will write streaming logs in S3 (every 5 minutes)
S3_LOG_PREFIX = "runtime-logs"   # s3://<BUCKET>/runtime-logs/<Name>/<InstanceId>/aer-scrape.log

# Scraper settings (you can edit defaults here)
DASHBOARDS = "Well_Summary_Report"   # or "Reservoir_Evaluation" or "Well_Gas_Analysis" or "all"
OUT_BASE = "/data/Data"
REMOTE_NAME = "s3aer"
PREFIX = "Data"                      # s3 folder under bucket
CODE_DIR = "/opt/aer-scraper/WebScraping/linux"
