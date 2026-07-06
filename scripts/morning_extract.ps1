# INTERIM (2026-07-06): local Toast extract + Volume upload.
# The trial workspace's serverless egress allowlist blocks the Toast API, so
# this runs on the local machine (Windows Task Scheduler, daily 08:45) until
# Databricks enables full egress -- then the bundle's in-cloud extract task is
# re-enabled and this script + its scheduled task are deleted.
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
