# Pipeline End-to-End Latency Benchmark Runner
$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  ULL-FLIG End-to-End Latency Benchmark Suite             " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Start or ensure native services are up
& "$PSScriptRoot\start-native.ps1"
Start-Sleep -Seconds 3

# 2. Wait for Gateway & DB connection to be fully ready
Write-Host "`n[Status] Probing Gateway Telemetry..." -ForegroundColor Yellow
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$ready = $false
while ($sw.Elapsed.TotalSeconds -lt 25) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8080/api/telemetry" -TimeoutSec 3 -ErrorAction Stop
        if ($resp.status -eq "ONLINE" -and $resp.dbConnected -eq $true) {
            $ready = $true
            Write-Host "  -> Gateway and Neon Cloud PostgreSQL are ONLINE & CONNECTED!" -ForegroundColor Green
            break
        }
    } catch {
        # Retry
    }
    Start-Sleep -Milliseconds 500
}

if (-not $ready) {
    throw "Gateway or Database was not ready within timeout. Check logs\audit.log and logs\gateway.log"
}

# 3. Execute PipelineLatencyBenchmark
Write-Host "`n[Benchmark] Executing Java End-to-End Latency Benchmark..." -ForegroundColor Yellow
$JavaExe = "C:\Program Files\Java\jdk-21\bin\java.exe"
$benchmarkCp = "$RootDir\target\test-classes;$RootDir\target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;$RootDir\target\dependency\*"

& $JavaExe -cp $benchmarkCp org.java.PipelineLatencyBenchmark

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  BENCHMARK EXECUTION COMPLETE!                           " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
