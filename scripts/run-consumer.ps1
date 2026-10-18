param (
    [string]$Id = "1",
    [string]$Seeds = "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $ScriptDir "..")

if ($Id -match '^\d+$') {
    $ConsumerId = "consumer-$Id"
} else {
    $ConsumerId = $Id
}

if ($ConsumerId -like "*2*") {
    $Topics = "payments,notifications,audit"
    $StatusPort = 9002
} else {
    $Topics = "orders,payments"
    $StatusPort = 9001
}

$DataDir = "data\$ConsumerId"
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

Write-Host "Starting $ConsumerId (topics=$Topics status_port=$StatusPort data_dir=$DataDir)..."
python cmd\consumer.py --consumer-id $ConsumerId --seeds $Seeds --topics $Topics --data-dir $DataDir --status-port $StatusPort $args
