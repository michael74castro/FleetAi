# Run Data Load Script
# Executes all SQL data load scripts against SQL Server

param(
    [string]$Server = "localhost",
    [string]$Database = "FleetAI",
    [switch]$TrustedConnection = $true,
    [string]$Username,
    [string]$Password
)

$DataDir = "$PSScriptRoot\..\data"
$SchemaDir = "$PSScriptRoot\..\schemas"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "FleetAI Data Load Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Server: $Server"
Write-Host "Database: $Database"
Write-Host ""

# Build connection string
if ($TrustedConnection) {
    $ConnArgs = "-S `"$Server`" -d `"$Database`" -E"
} else {
    $ConnArgs = "-S `"$Server`" -d `"$Database`" -U `"$Username`" -P `"$Password`""
}

# Step 1: Create schema and tables
Write-Host "Step 1: Creating landing schema and tables..." -ForegroundColor Yellow
$SchemaFile = Join-Path $SchemaDir "01_landing.sql"
if (Test-Path $SchemaFile) {
    $cmd = "sqlcmd $ConnArgs -i `"$SchemaFile`" -b"
    Write-Host "  Running: $cmd"
    Invoke-Expression $cmd
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR: Schema creation failed!" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Schema created successfully." -ForegroundColor Green
} else {
    Write-Host "  WARNING: Schema file not found at $SchemaFile" -ForegroundColor Yellow
}

Write-Host ""

# Step 2: Load data from each file
Write-Host "Step 2: Loading data from Excel exports..." -ForegroundColor Yellow
$SqlFiles = Get-ChildItem -Path $DataDir -Filter "load_*.sql" | Sort-Object Name

$TotalFiles = $SqlFiles.Count
$CurrentFile = 0
$FailedFiles = @()

foreach ($SqlFile in $SqlFiles) {
    $CurrentFile++
    $TableName = $SqlFile.BaseName -replace "load_", ""
    Write-Host "  [$CurrentFile/$TotalFiles] Loading $TableName..." -NoNewline

    $cmd = "sqlcmd $ConnArgs -i `"$($SqlFile.FullName)`" -b"
    $output = Invoke-Expression $cmd 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Host " FAILED" -ForegroundColor Red
        $FailedFiles += $TableName
    } else {
        Write-Host " OK" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Data Load Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total tables: $TotalFiles"
Write-Host "Successful: $($TotalFiles - $FailedFiles.Count)" -ForegroundColor Green

if ($FailedFiles.Count -gt 0) {
    Write-Host "Failed: $($FailedFiles.Count)" -ForegroundColor Red
    Write-Host "Failed tables: $($FailedFiles -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "All data loaded successfully!" -ForegroundColor Green
