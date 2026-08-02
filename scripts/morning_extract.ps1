# MANUAL FALLBACK (retired from scheduling 2026-08-02): local Toast extract +
# Volume upload. The scheduled daily extract now runs on GCP Cloud Run
# (`rfa-sync`, 08:30 CT); the Windows Task Scheduler entry that ran this at 08:45
# is disabled, not deleted. Kept because it is the proven gap-backfill tool --
# run it by hand to re-pull and re-upload the trailing 3 days when the cloud lane
# misses a morning. Uploads overwrite, so running it alongside Cloud Run is safe.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\morning_extract.ps1
#
# Original context: the trial workspace's serverless egress allowlist blocks the
# Toast API, so extraction cannot run inside Databricks at all -- it moved to
# Cloud Run rather than in-cloud. If Databricks ever enables full egress, the
# bundle's in-cloud extract task can be re-enabled and both local paths dropped.
$ErrorActionPreference = "Stop"
Set-Location "E:\PyProj\restaurant-forecast-analytics"
Start-Transcript -Path "logs\morning_extract.log" -Append

try {
    # 1. Re-pull the trailing 3 days from Toast (captures post-close edits).
    & .\.venv\Scripts\python.exe -m ingest.extract --refresh-days 3
    if ($LASTEXITCODE -ne 0) { throw "extract failed with exit $LASTEXITCODE" }

    # 2. Bridge .env -> Databricks CLI env (values never printed).
    Get-Content .env | ForEach-Object {
        if ($_ -match '^([A-Z_]+)=(.*)$') {
            Set-Item -Path "env:$($Matches[1])" -Value $Matches[2]
        }
    }
    $env:DATABRICKS_HOST = "https://$($env:DBX_HOST)"
    $env:DATABRICKS_TOKEN = $env:DBX_TOKEN
    $dbx = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\databricks.exe"

    # 3. Upload the 3 most recent partitions to the UC Volume.
    $parts = Get-ChildItem "data\raw\orders" -Directory | Sort-Object Name | Select-Object -Last 3
    foreach ($p in $parts) {
        & $dbx fs cp -r --overwrite $p.FullName "dbfs:/Volumes/workspace/default/raw_orders/$($p.Name)"
        if ($LASTEXITCODE -ne 0) { throw "upload of $($p.Name) failed" }
        Write-Output "uploaded $($p.Name)"
    }
    Write-Output "morning extract + upload complete"
} finally {
    Stop-Transcript
}
