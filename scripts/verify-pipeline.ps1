# Comprehensive Phase 1 Pipeline & Recovery Verification Script
$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Phase 1: Pipeline Stabilization & Verification Suite    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Function to read 64-bit unsigned integer at offset from a binary ring file
function Get-RingCounter([string]$FilePath, [long]$Offset) {
    if (-not (Test-Path $FilePath)) { return -1 }
    $stream = [System.IO.File]::Open($FilePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $stream.Seek($Offset, [System.IO.SeekOrigin]::Begin) | Out-Null
        $reader = New-Object System.IO.BinaryReader($stream)
        return $reader.ReadUInt64()
    } finally {
        $stream.Close()
    }
}

# Function to read 8-byte magic from ring header
function Get-RingMagic([string]$FilePath) {
    if (-not (Test-Path $FilePath)) { return "" }
    $stream = [System.IO.File]::Open($FilePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $reader = New-Object System.IO.BinaryReader($stream)
        $bytes = $reader.ReadBytes(8)
        return [System.Text.Encoding]::ASCII.GetString($bytes)
    } finally {
        $stream.Close()
    }
}

# 1. Clean previous state
Write-Host "`n[Step 1] Stopping existing processes and cleaning transient ring files..." -ForegroundColor Yellow
& "$PSScriptRoot\stop-native.ps1"
Remove-Item ".\*.ring" -Force -ErrorAction SilentlyContinue

# 2. Start services
Write-Host "`n[Step 2] Starting pipeline cluster..." -ForegroundColor Yellow
& "$PSScriptRoot\start-native.ps1"
Start-Sleep -Seconds 2

# 3. Check ring file geometry and magic
Write-Host "`n[Step 3] Verifying Ring File Geometries & Magic..." -ForegroundColor Yellow
$inboundSize = (Get-Item ".\g12-inbound.ring").Length
$journalSize = (Get-Item ".\g12-journal.ring").Length
$ackSize = (Get-Item ".\g12-ack.ring").Length

Write-Host "  Inbound ring size: $inboundSize bytes (expected 8388800)"
Write-Host "  Journal ring size: $journalSize bytes (expected 67109056)"
Write-Host "  ACK ring size:     $ackSize bytes (expected 4194496)"

if ($inboundSize -ne 8388800) { throw "Inbound ring size mismatch" }
if ($journalSize -ne 67109056) { throw "Journal ring size mismatch" }
if ($ackSize -ne 4194496) { throw "ACK ring size mismatch" }

$inboundMagic = Get-RingMagic ".\g12-inbound.ring"
$journalMagic = Get-RingMagic ".\g12-journal.ring"
$ackMagic = Get-RingMagic ".\g12-ack.ring"
Write-Host "  Inbound Magic: $inboundMagic, Journal Magic: $journalMagic, ACK Magic: $ackMagic"
if ($inboundMagic -ne "G12RING1" -or $journalMagic -ne "G12RING1" -or $ackMagic -ne "G12RING1") {
    throw "Ring magic header mismatch"
}
Write-Host "  -> Ring geometries & headers verified!" -ForegroundColor Green

# Initial counters before test
$inbound_p0 = Get-RingCounter ".\g12-inbound.ring" 64
$inbound_c0 = Get-RingCounter ".\g12-inbound.ring" 128
$journal_p0 = Get-RingCounter ".\g12-journal.ring" 64
$journal_c0 = Get-RingCounter ".\g12-journal.ring" 128
$journal_stream0 = Get-RingCounter ".\g12-journal.ring" 32
$ack_p0 = Get-RingCounter ".\g12-ack.ring" 64
$ack_c0 = Get-RingCounter ".\g12-ack.ring" 128

Write-Host "`n[Step 4] Baseline Ring Counters:" -ForegroundColor Yellow
Write-Host "  Inbound: producer=$inbound_p0, consumer=$inbound_c0"
Write-Host "  Journal: producer=$journal_p0, consumer=$journal_c0, streamId=$journal_stream0"
Write-Host "  ACK:     producer=$ack_p0, consumer=$ack_c0"

# 4. Run test client
Write-Host "`n[Step 5] Executing GatewayTestClient (valid transfer, zero amount, malformed)..." -ForegroundColor Yellow
$clientCp = "target\test-classes;target\Netty-Ingress-Gateway-1.0-SNAPSHOT.jar;target\dependency\*"
$env:G12_GATEWAY_URI = "ws://localhost:8080/ws"
$env:G12_JOURNAL_RING_PATH = ".\g12-journal.ring"
& "C:\Program Files\Java\jdk-21\bin\java.exe" -cp $clientCp org.java.GatewayTestClient
if ($LASTEXITCODE -ne 0) { throw "GatewayTestClient failed" }

# 5. Verify counter advancement
Start-Sleep -Milliseconds 500
$inbound_p1 = Get-RingCounter ".\g12-inbound.ring" 64
$inbound_c1 = Get-RingCounter ".\g12-inbound.ring" 128
$journal_p1 = Get-RingCounter ".\g12-journal.ring" 64
$journal_c1 = Get-RingCounter ".\g12-journal.ring" 128
$ack_p1 = Get-RingCounter ".\g12-ack.ring" 64
$ack_c1 = Get-RingCounter ".\g12-ack.ring" 128

Write-Host "`n[Step 6] Verifying SPSC Monotonic Advancements:" -ForegroundColor Yellow
Write-Host "  Inbound: producer $inbound_p0 -> $inbound_p1, consumer $inbound_c0 -> $inbound_c1"
Write-Host "  Journal: producer $journal_p0 -> $journal_p1, consumer $journal_c0 -> $journal_c1"
Write-Host "  ACK:     producer $ack_p0 -> $ack_p1, consumer $ack_c0 -> $ack_c1"

if ($inbound_p1 -le $inbound_p0 -or $inbound_c1 -le $inbound_c0) { throw "Inbound counters failed to advance" }
if ($journal_p1 -le $journal_p0 -or $journal_c1 -le $journal_c0) { throw "Journal counters failed to advance" }
if ($ack_p1 -le $ack_p0 -or $ack_c1 -le $ack_c0) { throw "ACK counters failed to advance" }
Write-Host "  -> All 3 ring pairs advanced monotonically!" -ForegroundColor Green

# 6. Test Service Shutdown & Restart Preservation
Write-Host "`n[Step 7] Testing Clean Shutdown & Counter Preservation on Restart..." -ForegroundColor Yellow
& "$PSScriptRoot\stop-native.ps1"
Start-Sleep -Seconds 1

# Restart without deleting ring files
& "$PSScriptRoot\start-native.ps1"
Start-Sleep -Seconds 2

$inbound_p2 = Get-RingCounter ".\g12-inbound.ring" 64
$journal_p2 = Get-RingCounter ".\g12-journal.ring" 64
$ack_p2 = Get-RingCounter ".\g12-ack.ring" 64
$journal_stream2 = Get-RingCounter ".\g12-journal.ring" 32

Write-Host "  Counters after restart:"
Write-Host "  Inbound producer: $inbound_p2 (expected $inbound_p1)"
Write-Host "  Journal producer: $journal_p2 (expected $journal_p1)"
Write-Host "  ACK producer:     $ack_p2 (expected $ack_p1)"
Write-Host "  Journal streamId: $journal_stream2 (expected $journal_stream0)"

if ($inbound_p2 -ne $inbound_p1) { throw "Inbound producer counter reset after restart" }
if ($journal_p2 -ne $journal_p1) { throw "Journal producer counter reset after restart" }
if ($ack_p2 -ne $ack_p1) { throw "ACK producer counter reset after restart" }
if ($journal_stream2 -ne $journal_stream0) { throw "Journal streamId changed after restart" }
Write-Host "  -> Restart preservation verified! Rings and streamId survived restart." -ForegroundColor Green

# 7. Final shutdown
Write-Host "`n[Step 8] Stopping all services..." -ForegroundColor Yellow
& "$PSScriptRoot\stop-native.ps1"

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  PHASE 1 ACCEPTANCE CRITERIA FULLY VERIFIED!             " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
