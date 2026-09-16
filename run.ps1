# Emergency Bridge Launcher (PowerShell)
$Host.UI.RawUI.WindowTitle = "Emergency Bridge MAX <-> Telegram"
Set-Location -Path $PSScriptRoot

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "  Emergency Bridge: MAX Messenger <-> Telegram" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan
Write-Host ""

if (-not (Test-Path ".env")) {
    Write-Host "[ERROR] File .env not found!" -ForegroundColor Red
    Write-Host "Please copy .env.example to .env and fill in your tokens." -ForegroundColor Yellow
    Write-Host ""
    pause
    exit 1
}

$pythonExe = "python"
if (Test-Path "c:\Git\sd.mrdn.engineering\.venv\Scripts\python.exe") {
    $pythonExe = "c:\Git\sd.mrdn.engineering\.venv\Scripts\python.exe"
}

Write-Host "[OK] Using Python: $pythonExe" -ForegroundColor Green
Write-Host "[OK] Starting bridge.py..." -ForegroundColor Green
Write-Host ""

& $pythonExe bridge.py

