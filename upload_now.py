#!/usr/bin/env python
"""Quick script to upload database to Google Drive"""
import subprocess
import os

DB_PATH = r'c:\Users\mrdea\company_enricher\CompanyEnricher\companies.db'
RCLONE_PATH = r'C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe'

# Check file exists
if os.path.exists(DB_PATH):
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"Database found: {size_mb:.1f} MB")
else:
    print("ERROR: Database not found!")
    exit(1)

# Check rclone exists
if os.path.exists(RCLONE_PATH):
    print(f"Rclone found at: {RCLONE_PATH}")
else:
    print("ERROR: Rclone not found!")
    exit(1)

print(f"\nUploading to Google Drive...")
result = subprocess.run([
    RCLONE_PATH, 'copy',
    DB_PATH,
    'gdrive:CompanyEnricher/',
    '-v'
], capture_output=True, text=True)

print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print(f"Return code: {result.returncode}")

if result.returncode == 0:
    print("\n✅ Upload completed successfully!")
    # Verify
    print("\nVerifying upload...")
    verify = subprocess.run([
        RCLONE_PATH, 'lsl',
        'gdrive:CompanyEnricher/'
    ], capture_output=True, text=True)
    print("Files in Google Drive:")
    print(verify.stdout)
else:
    print(f"\n❌ Upload failed with return code {result.returncode}")
