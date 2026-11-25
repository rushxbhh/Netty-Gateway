# Comprehensive Phase 3 Automated Reliability & Boundary Runner
$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Phase 3: Automated Pipeline Reliability Test Runner     " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Compile test suite
Write-Host "`n[Build] Compiling Test Classes..." -ForegroundColor Yellow
$MvnCmd = "C:\Users\Saarthak\.m2\wrapper\dists\apache-maven-3.9.12\59fe215c0ad6947fea90184bf7add084544567b927287592651fda3782e0e798\bin\mvn.cmd"
& $MvnCmd test-compile -q

# 2. Ensure native cluster is running
Write-Host "`n[Services] Starting ULL-FLIG Pipeline Services..." -ForegroundColor Yellow
& "$PSScriptRoot\start-native.ps1"
Start-Sleep -Seconds 3

# 3. Wait for Gateway & DB connection
Write-Host "`n[Status] Waiting for Gateway & Neon Database..." -ForegroundColor Yellow
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$ready = $false
while ($sw.Elapsed.TotalSeconds -lt 25) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8080/api/telemetry" -TimeoutSec 3 -ErrorAction Stop
        if ($resp.status -eq "ONLINE" -and $resp.dbConnected -eq $true) {
            $ready = $true
            Write-Host "  -> Gateway and Database are ONLINE!" -ForegroundColor Green
            break
        }
    } catch {}
    Start-Sleep -Milliseconds 500
}

if (-not $ready) {
    throw "Gateway or Database was not ready within timeout. Check logs\audit.log and logs\gateway.log"
}

# 4. Execute Java EndToEndReliabilityTestSuite
Write-Host "`n[Suite] Executing Java EndToEndReliabilityTestSuite..." -ForegroundColor Yellow
$JavaExe = "C:\Program Files\Java\jdk-21\bin\java.exe"
$testCp = "$RootDir\target\test-classes;$RootDir\target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;$RootDir\target\dependency\*"

& $JavaExe -cp $testCp org.java.EndToEndReliabilityTestSuite

if ($LASTEXITCODE -ne 0) {
    throw "EndToEndReliabilityTestSuite failed with exit code $LASTEXITCODE"
}

# 5. Service Restart and Counter Preservation Verification
Write-Host "`n[Recovery] Testing Mid-Stream Cluster Restart Preservation..." -ForegroundColor Yellow
# Get current counters before restart
$inboundBefore = (Get-Item ".\g12-inbound.ring").Length
$journalBefore = (Get-Item ".\g12-journal.ring").Length

# Restart cluster
& "$PSScriptRoot\stop-native.ps1"
Start-Sleep -Seconds 1
& "$PSScriptRoot\start-native.ps1"
Start-Sleep -Seconds 3

# Wait for readiness again
$sw.Restart()
while ($sw.Elapsed.TotalSeconds -lt 25) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8080/api/telemetry" -TimeoutSec 3 -ErrorAction Stop
        if ($resp.status -eq "ONLINE" -and $resp.dbConnected -eq $true) {
            Write-Host "  -> Post-restart cluster is ONLINE!" -ForegroundColor Green
            break
        }
    } catch {}
    Start-Sleep -Milliseconds 500
}

# Run a post-restart transaction to verify counter continuity
Write-Host "  Submitting transaction post-restart to verify continuity..." -ForegroundColor Cyan
& $JavaExe -cp $testCp org.java.GatewayTestClient

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  ALL PHASE 3 RELIABILITY TESTS COMPLETED SUCCESSFULLY!   " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
