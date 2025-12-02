#!/usr/bin/env python
"""Upload database to Google Drive"""
import subprocess
import os
import glob

RCLONE = r'C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe'
DIR = r'c:\Users\mrdea\company_enricher\CompanyEnricher'

# Find database files
print("Looking for database files...")
db_files = []
for f in os.listdir(DIR):
    if f.endswith('.db'):
        full_path = os.path.join(DIR, f)
        size_mb = os.path.getsize(full_path) / (1024 * 1024)
        db_files.append((f, full_path, size_mb))
        print(f"  Found: {f} ({size_mb:.1f} MB)")

if not db_files:
    print("No .db files found!")
    exit(1)

# Find the main companies.db or the most recent enriched file
main_db = None
for name, path, size in db_files:
    if name == 'companies.db':
        main_db = path
        break

if not main_db:
    # Use the most recent enriched file
    enriched = [x for x in db_files if 'enriched' in x[0]]
    if enriched:
        # Sort by name (which has timestamp)
        enriched.sort(key=lambda x: x[0], reverse=True)
        main_db = enriched[0][1]
        print(f"\nUsing most recent enriched backup: {enriched[0][0]}")
    else:
        main_db = db_files[0][1]

if main_db:
    size_mb = os.path.getsize(main_db) / (1024 * 1024)
    print(f"\nUploading: {os.path.basename(main_db)} ({size_mb:.1f} MB)")
    print("Running rclone...")
    
    result = subprocess.run([
        RCLONE, 'copy',
        main_db,
        'gdrive:CompanyEnricher/',
        '-v', '--progress'
    ], capture_output=False)
    
    print(f"\nrclone exit code: {result.returncode}")
    
    if result.returncode == 0:
        print("\n✅ Upload completed!")
        print("\nVerifying...")
        subprocess.run([RCLONE, 'lsl', 'gdrive:CompanyEnricher/'])
    else:
        print(f"\n❌ Upload failed")
