import subprocess
import os

# Paths
RCLONE = r'C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe'
DB_DIR = r'c:\Users\mrdea\company_enricher\CompanyEnricher'
LOG = r'c:\Users\mrdea\company_enricher\CompanyEnricher\upload_result.txt'

with open(LOG, 'w') as log:
    log.write("=== Upload Script Started ===\n")
    
    # List directory contents
    log.write(f"\nDirectory listing of {DB_DIR}:\n")
    for f in os.listdir(DB_DIR):
        if f.endswith('.db'):
            path = os.path.join(DB_DIR, f)
            size = os.path.getsize(path) / (1024*1024)
            log.write(f"  {f}: {size:.1f} MB\n")
    
    # Find the main db
    db_path = os.path.join(DB_DIR, 'companies.db')
    if not os.path.exists(db_path):
        # Find most recent enriched
        enriched = [f for f in os.listdir(DB_DIR) if f.endswith('.db') and 'enriched' in f]
        if enriched:
            enriched.sort(reverse=True)
            db_path = os.path.join(DB_DIR, enriched[0])
        else:
            # Any db file
            dbs = [f for f in os.listdir(DB_DIR) if f.endswith('.db')]
            if dbs:
                db_path = os.path.join(DB_DIR, dbs[0])
    
    if os.path.exists(db_path):
        size_mb = os.path.getsize(db_path) / (1024*1024)
        log.write(f"\nUploading: {db_path} ({size_mb:.1f} MB)\n")
        
        # Run rclone
        log.write(f"\nRunning: {RCLONE} copy {db_path} gdrive:CompanyEnricher/ -v\n")
        result = subprocess.run(
            [RCLONE, 'copy', db_path, 'gdrive:CompanyEnricher/', '-v'],
            capture_output=True, text=True
        )
        
        log.write(f"\nReturn code: {result.returncode}\n")
        log.write(f"\nSTDOUT:\n{result.stdout}\n")
        log.write(f"\nSTDERR:\n{result.stderr}\n")
        
        if result.returncode == 0:
            log.write("\n=== SUCCESS ===\n")
            # Verify
            log.write("\nVerifying...\n")
            verify = subprocess.run(
                [RCLONE, 'lsl', 'gdrive:CompanyEnricher/'],
                capture_output=True, text=True
            )
            log.write(f"Files in gdrive:CompanyEnricher/:\n{verify.stdout}\n")
        else:
            log.write("\n=== FAILED ===\n")
    else:
        log.write(f"\nNo database file found!\n")
    
    log.write("\n=== Script Finished ===\n")

print("Done - check upload_result.txt")
