# scripts/start-cluster.ps1
# Start all 5 Raft nodes in the background on Windows using PowerShell

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
New-Item -ItemType Directory -Force -Path "$root\data" | Out-Null

$peers = "1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005"

1..5 | ForEach-Object {
    $id = $_
    $port = 8000 + $id
    $dataDir = "$root\data\node-$id"
    New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
    
    $logFile = "$root\logs\node-$id.log"
    $errFile = "$root\logs\node-$id.err.log"
    $args = "-m broker.broker --id $id --port $port --peers $peers --data-dir $dataDir --log-level INFO --election-min-ms 5000 --election-max-ms 8000 --heartbeat-ms 300"
    
    Write-Host "Starting Node $id on port $port..."
    Start-Process -FilePath "python" -ArgumentList $args -WorkingDirectory $root -RedirectStandardOutput $logFile -RedirectStandardError $errFile -WindowStyle Hidden
}

Start-Sleep -Seconds 2
Write-Host "`nAll 5 nodes launched in background."
Write-Host "Dashboard: Start-Process raft_dashboard.html"
Write-Host "Or browser: http://localhost:8001/"
