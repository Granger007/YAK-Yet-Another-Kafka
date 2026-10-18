param (
    [string]$ProducerId = "prod-1",
    [string]$Seeds = "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005",
    [string]$Topics = "orders,payments,notifications,audit",
    [int]$Count = 50,
    [double]$Interval = 0.2,
    [string]$Ledger = "data\producer.ledger"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $ScriptDir "..")

$LedgerDir = Split-Path -Parent $Ledger
if (-not (Test-Path $LedgerDir)) {
    New-Item -ItemType Directory -Path $LedgerDir -Force | Out-Null
}

Write-Host "Starting producer $ProducerId (topics=$Topics count=$Count seeds=$Seeds)..."
python cmd\producer.py --producer-id $ProducerId --seeds $Seeds --topics $Topics --count $Count --interval $Interval --ledger $Ledger $args
