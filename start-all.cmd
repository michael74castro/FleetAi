@echo off
echo Starting FleetAI Development Environment...
start "FleetAI Backend" cmd /k "cd /d %~dp0backend && python -m uvicorn main:app --reload --port 8000"
timeout /t 3 /nobreak >nul
start "FleetAI Frontend" cmd /k "cd /d %~dp0frontend && npm run dev"
echo.
echo Backend starting at: http://localhost:8000
echo Frontend starting at: http://localhost:3000
