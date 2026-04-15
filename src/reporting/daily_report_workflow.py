# src/reporting/daily_report_workflow.py

# crontab -e
# 0 2 * * * /usr/bin/python3 /path/to/project/src/reporting/daily_report_workflow.py
# Daily 2 AM run

import subprocess
import sys
from datetime import datetime

def run_step(name, cmd):
    print(f"[{datetime.now()}] Running: {name}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"{name} failed")

def main():
    run_step("Load S3 data into Snowflake", "python src/reporting/load_s3_to_snowflake.py")
    run_step("Build daily report", "python src/reporting/build_daily_report.py")
    print(f"[{datetime.now()}] Daily reporting workflow completed successfully.")

if __name__ == "__main__":
    main()