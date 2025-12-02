# PowerShell script to upload database to Google Drive
$ErrorActionPreference = "Stop"

$rclone = "C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe"
$dbDir = "c:\Users\mrdea\company_enricher\CompanyEnricher"
$logFile = "c:\Users\mrdea\company_enricher\CompanyEnricher\gdrive_upload.log"

# Start log
"Upload started at $(Get-Date)" | Out-File $logFile

# Find database files
"Looking for .db files in $dbDir" | Tee-Object -FilePath $logFile -Append
$dbFiles = Get-ChildItem -Path $dbDir -Filter "*.db" -ErrorAction SilentlyContinue

if ($dbFiles) {
    foreach ($f in $dbFiles) {
        $sizeMB = [math]::Round($f.Length / 1MB, 1)
        "  Found: $($f.Name) ($sizeMB MB)" | Tee-Object -FilePath $logFile -Append
    }
    
    # Prefer companies.db, else use most recent enriched
    $mainDb = $dbFiles | Where-Object { $_.Name -eq "companies.db" } | Select-Object -First 1
    if (-not $mainDb) {
        $mainDb = $dbFiles | Where-Object { $_.Name -like "*enriched*" } | Sort-Object Name -Descending | Select-Object -First 1
    }
    if (-not $mainDb) {
        $mainDb = $dbFiles | Select-Object -First 1
    }
    
    $sizeMB = [math]::Round($mainDb.Length / 1MB, 1)
    "Uploading: $($mainDb.FullName) ($sizeMB MB)" | Tee-Object -FilePath $logFile -Append
    
    # Run rclone
    $proc = Start-Process -FilePath $rclone -ArgumentList "copy", $mainDb.FullName, "gdrive:CompanyEnricher/", "-v" -Wait -PassThru -NoNewWindow -RedirectStandardOutput "$logFile.stdout" -RedirectStandardError "$logFile.stderr"
    
    "Rclone exit code: $($proc.ExitCode)" | Tee-Object -FilePath $logFile -Append
    
    if (Test-Path "$logFile.stdout") {
        Get-Content "$logFile.stdout" | Tee-Object -FilePath $logFile -Append
    }
    if (Test-Path "$logFile.stderr") {
        Get-Content "$logFile.stderr" | Tee-Object -FilePath $logFile -Append
    }
    
    if ($proc.ExitCode -eq 0) {
        "SUCCESS: Upload completed!" | Tee-Object -FilePath $logFile -Append
        
        # Verify
        "Verifying upload..." | Tee-Object -FilePath $logFile -Append
        & $rclone lsl "gdrive:CompanyEnricher/" 2>&1 | Tee-Object -FilePath $logFile -Append
    } else {
        "FAILED: Upload failed with exit code $($proc.ExitCode)" | Tee-Object -FilePath $logFile -Append
    }
} else {
    "ERROR: No .db files found in $dbDir" | Tee-Object -FilePath $logFile -Append
}

"Done at $(Get-Date)" | Tee-Object -FilePath $logFile -Append
