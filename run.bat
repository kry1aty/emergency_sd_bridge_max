@echo off
cd /d "%~dp0"
title Emergency Bridge MAX ^<^-^> Telegram

echo ===================================================
echo   Emergency Bridge: MAX Messenger ^<^-^> Telegram
echo ===================================================
echo.

if not exist ".env" (
    echo [ERROR] File .env not found!
    echo Please copy .env.example to .env and fill in your tokens.
    echo.
    pause
    exit /b 1
)

set "PYTHON_EXE="
if exist "c:\Git\sd.mrdn.engineering\.venv\Scripts\python.exe" (
    set "PYTHON_EXE=c:\Git\sd.mrdn.engineering\.venv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

echo [OK] Using Python: %PYTHON_EXE%
echo [OK] Starting bridge.py...
echo.

"%PYTHON_EXE%" bridge.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Bridge stopped with error code %errorlevel%.
    pause
)
