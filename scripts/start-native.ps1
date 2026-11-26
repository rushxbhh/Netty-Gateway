# Starts all 3 ULL-FLIG services natively on Windows
param (
    [switch]$BuildFirst = $false,
    [int]$GatewayPort = 8080
)

$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting ULL-FLIG Pipeline Services Natively (Windows)  " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Load .env if present
if (Test-Path "$RootDir\.env") {
    Write-Host "[Config] Loading environment variables from .env" -ForegroundColor Green
    Get-Content "$RootDir\.env" | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $parts = $line.Split("=", 2)
            [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
        }
    }
}

$JavaHome = "C:\Program Files\Java\jdk-21"
$JavaExe = "$JavaHome\bin\java.exe"
$MvnCmd = "C:\Users\Saarthak\.m2\wrapper\dists\apache-maven-3.9.12\59fe215c0ad6947fea90184bf7add084544567b927287592651fda3782e0e798\bin\mvn.cmd"
$GppExe = "C:\msys64\ucrt64\bin\g++.exe"

if (-not (Test-Path $JavaExe)) {
    $JavaExe = "java.exe"
}

# Optional build
if ($BuildFirst -or (-not (Test-Path "$RootDir\sequencer\g12_sequencer.exe"))) {
    Write-Host "[Build] Compiling C++ Sequencer..." -ForegroundColor Yellow
    & $GppExe -std=c++20 -O3 "-msse4.2" -Wall -Wextra -Wpedantic -I "$RootDir\sequencer\include" "$RootDir\sequencer\src\main.cpp" -o "$RootDir\sequencer\g12_sequencer.exe"
    if ($LASTEXITCODE -ne 0) { throw "Failed to build g12_sequencer.exe" }
}

if ($BuildFirst -or (-not (Test-Path "$RootDir\target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar")) -or (-not (Test-Path "$RootDir\target\dependency"))) {
    Write-Host "[Build] Packaging Gateway..." -ForegroundColor Yellow
    & $MvnCmd package dependency:copy-dependencies -DoutputDirectory=target/dependency -DskipTests
    if ($LASTEXITCODE -ne 0) { throw "Failed to package Gateway" }
}

if ($BuildFirst -or (-not (Test-Path "$RootDir\audit-service\target\g12-audit-service-1.0-SNAPSHOT.jar")) -or (-not (Test-Path "$RootDir\audit-service\target\dependency"))) {
    Write-Host "[Build] Packaging Audit Service..." -ForegroundColor Yellow
    & $MvnCmd -f "$RootDir\audit-service\pom.xml" package dependency:copy-dependencies -DoutputDirectory=target/dependency -DskipTests
    if ($LASTEXITCODE -ne 0) { throw "Failed to package Audit Service" }
}

# Stop any previously running instances
& "$PSScriptRoot\stop-native.ps1"

# Inbound & Journal ring paths
$inboundRing = if ($env:G12_INBOUND_RING_PATH) { $env:G12_INBOUND_RING_PATH } else { ".\g12-inbound.ring" }
$journalRing = if ($env:G12_JOURNAL_RING_PATH) { $env:G12_JOURNAL_RING_PATH } else { ".\g12-journal.ring" }
$ackRing = if ($env:G12_ACK_RING_PATH) { $env:G12_ACK_RING_PATH } else { ".\g12-ack.ring" }

$logDir = "$RootDir\logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

Write-Host "`n[1/3] Launching C++ Sequencer Daemon..." -ForegroundColor Yellow
$seqProcess = Start-Process -FilePath "$RootDir\sequencer\g12_sequencer.exe" `
    -ArgumentList "--no-pin", "--inbound", $inboundRing, "--journal", $journalRing `
    -RedirectStandardOutput "$logDir\sequencer.log" `
    -RedirectStandardError "$logDir\sequencer_err.log" `
    -PassThru -NoNewWindow
Start-Sleep -Milliseconds 800

if ($seqProcess.HasExited) {
    throw "C++ Sequencer terminated immediately with exit code $($seqProcess.ExitCode)"
}
Write-Host "  -> C++ Sequencer PID: $($seqProcess.Id)" -ForegroundColor Green

Write-Host "`n[2/3] Launching Java Audit Service..." -ForegroundColor Yellow
$auditCp = "$RootDir\audit-service\target\g12-audit-service-1.0-SNAPSHOT.jar;$RootDir\audit-service\target\dependency\*"
$env:G12_JOURNAL_RING_PATH = $journalRing
$env:G12_ACK_RING_PATH = $ackRing

$auditProcess = Start-Process -FilePath $JavaExe `
    -ArgumentList "-cp `"$auditCp`" org.g12.audit.AuditApplication" `
    -RedirectStandardOutput "$logDir\audit.log" `
    -RedirectStandardError "$logDir\audit_err.log" `
    -PassThru -NoNewWindow

Write-Host "  Waiting for Audit Service to connect to DB and create ACK ring..." -ForegroundColor Cyan
$timeoutSec = 30
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while (-not (Test-Path $ackRing) -and $sw.Elapsed.TotalSeconds -lt $timeoutSec) {
    if ($auditProcess.HasExited) {
        throw "Java Audit Service terminated unexpectedly with exit code $($auditProcess.ExitCode)"
    }
    Start-Sleep -Milliseconds 300
}
if (-not (Test-Path $ackRing)) {
    throw "Timeout waiting for Audit Service to create $ackRing"
}
Write-Host "  -> Java Audit Service is ready (PID: $($auditProcess.Id))" -ForegroundColor Green

Write-Host "`n[3/3] Launching Netty WebSocket Gateway..." -ForegroundColor Yellow
$gatewayCp = "$RootDir\target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;$RootDir\target\dependency\*"
$env:G12_INBOUND_RING_PATH = $inboundRing
$env:G12_ACK_RING_PATH = $ackRing
$env:G12_JOURNAL_RING_PATH = $journalRing

$gatewayProcess = Start-Process -FilePath $JavaExe `
    -ArgumentList "-cp `"$gatewayCp`" org.java.GatewayServer $GatewayPort" `
    -RedirectStandardOutput "$logDir\gateway.log" `
    -RedirectStandardError "$logDir\gateway_err.log" `
    -PassThru -NoNewWindow
Start-Sleep -Seconds 2

if ($gatewayProcess.HasExited) {
    throw "Netty Gateway terminated unexpectedly with exit code $($gatewayProcess.ExitCode)"
}
Write-Host "  -> Netty Gateway PID: $($gatewayProcess.Id)" -ForegroundColor Green

# Save PIDs to a file for clean shutdown
Set-Content -Path "$RootDir\.pids.json" -Value (@{
    sequencer = $seqProcess.Id
    audit = $auditProcess.Id
    gateway = $gatewayProcess.Id
} | ConvertTo-Json)

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  ULL-FLIG Pipeline is running natively!                 " -ForegroundColor Green
Write-Host "  WebSocket Gateway: ws://localhost:$GatewayPort/ws       " -ForegroundColor Green
Write-Host "  Frontend Hosted: http://localhost:$GatewayPort       " -ForegroundColor Green
Write-Host "  To stop all services: .\scripts\stop-native.ps1         " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
