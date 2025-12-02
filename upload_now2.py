#!/usr/bin/env python
"""Quick script to upload database to Google Drive"""
import subprocess
import os
import sys

# Force flush
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

DB_PATH = r'c:\Users\mrdea\company_enricher\CompanyEnricher\companies.db'
RCLONE_PATH = r'C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe'
LOG_FILE = r'c:\Users\mrdea\company_enricher\CompanyEnricher\upload_log.txt'

log = []

# Check file exists
if os.path.exists(DB_PATH):
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    log.append(f"Database found: {size_mb:.1f} MB")
else:
    log.append("ERROR: Database not found!")
    with open(LOG_FILE, 'w') as f:
        f.write('\n'.join(log))
    exit(1)

# Check rclone exists
if os.path.exists(RCLONE_PATH):
    log.append(f"Rclone found")
else:
    log.append("ERROR: Rclone not found!")
    with open(LOG_FILE, 'w') as f:
        f.write('\n'.join(log))
    exit(1)

log.append("Uploading to Google Drive...")

result = subprocess.run([
    RCLONE_PATH, 'copy',
    DB_PATH,
    'gdrive:CompanyEnricher/',
    '-v'
], capture_output=True, text=True)

log.append(f"STDOUT: {result.stdout}")
log.append(f"STDERR: {result.stderr}")
log.append(f"Return code: {result.returncode}")

if result.returncode == 0:
    log.append("Upload completed successfully!")
    # Verify
    verify = subprocess.run([
        RCLONE_PATH, 'lsl',
        'gdrive:CompanyEnricher/'
    ], capture_output=True, text=True)
    log.append("Files in Google Drive:")
    log.append(verify.stdout)
else:
    log.append(f"Upload failed with return code {result.returncode}")

# Write to log file
with open(LOG_FILE, 'w', encoding='utf-8') as f:
    f.write('\n'.join(log))

print("Done - check upload_log.txt")
