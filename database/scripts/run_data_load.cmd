@echo off
REM FleetAI Data Load Script
REM Loads initial data from Excel exports into SQL Server

SET SERVER=localhost
SET DATABASE=FleetAI
SET DATA_DIR=%~dp0..\data
SET SCHEMA_DIR=%~dp0..\schemas

echo ========================================
echo FleetAI Data Load Script
echo ========================================
echo Server: %SERVER%
echo Database: %DATABASE%
echo.

REM Step 1: Create schema
echo Step 1: Creating landing schema and tables...
sqlcmd -S %SERVER% -d %DATABASE% -E -i "%SCHEMA_DIR%\01_landing.sql" -b
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Schema creation failed!
    exit /b 1
)
echo Schema created successfully.
echo.

REM Step 2: Load data
echo Step 2: Loading data...
echo.

for %%f in ("%DATA_DIR%\load_*.sql") do (
    echo Loading %%~nf...
    sqlcmd -S %SERVER% -d %DATABASE% -E -i "%%f" -b
    if %ERRORLEVEL% NEQ 0 (
        echo WARNING: Failed to load %%~nf
    )
)

echo.
echo ========================================
echo Data load complete!
echo ========================================
