$RootDir = Split-Path -Parent $PSScriptRoot

Write-Host "Stopping running ULL-FLIG services..." -ForegroundColor Yellow

if (Test-Path "$RootDir\.pids.json") {
    try {
        $pids = Get-Content "$RootDir\.pids.json" | ConvertFrom-Json
        foreach ($name in @("gateway", "audit", "sequencer")) {
            $id = $pids.$name
            if ($id) {
                $proc = Get-Process -Id $id -ErrorAction SilentlyContinue
                if ($proc) {
                    Write-Host "  Stopping $name (PID $id)..."
                    Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
                }
            }
        }
    } catch {
        # ignore parse error
    }
    Remove-Item "$RootDir\.pids.json" -Force -ErrorAction SilentlyContinue
}

# Also ensure any lingering g12_sequencer.exe or matching java processes are halted
Get-Process -Name "g12_sequencer" -ErrorAction SilentlyContinue | ForEach-Object {
    Write-Host "  Terminating remaining g12_sequencer (PID $($_.Id))..."
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

Write-Host "All native services stopped." -ForegroundColor Green
