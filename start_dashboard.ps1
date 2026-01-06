# Contract Oversight System - Startup Script
# Run this script to start both web dashboards:
#   - Main Dashboard (port 5002)
#   - Surtax Oversight Dashboard (port 5847)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Contract Oversight System" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Starting both dashboards..." -ForegroundColor Cyan
Write-Host ""

# Get script directory
$scriptDir = $PSScriptRoot
if (-not $scriptDir) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}

# Configuration
$MainPort = 5002
$SurtaxPort = 5847
$AutoOpenBrowser = $true

# Check for virtual environment
$venvPath = Join-Path $scriptDir "venv"
$venvActivate = Join-Path $venvPath "Scripts\Activate.ps1"

if (Test-Path $venvActivate) {
    Write-Host "Activating virtual environment..." -ForegroundColor Yellow
    & $venvActivate
} else {
    Write-Host "No virtual environment found, using system Python" -ForegroundColor Yellow
}

# Check Python is available
Write-Host "Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python 3.8+ from https://python.org" -ForegroundColor Red
    exit 1
}

# Check required dependencies
Write-Host "Checking dependencies..." -ForegroundColor Yellow

$requiredPackages = @(
    @{Name="flask"; Import="flask"},
    @{Name="flask-cors"; Import="flask_cors"},
    @{Name="pandas"; Import="pandas"},
    @{Name="plotly"; Import="plotly"}
)

$missingPackages = @()

foreach ($pkg in $requiredPackages) {
    $checkCmd = "import $($pkg.Import)"
    $result = python -c $checkCmd 2>&1
    if ($LASTEXITCODE -ne 0) {
        $missingPackages += $pkg.Name
        Write-Host "  Missing: $($pkg.Name)" -ForegroundColor Red
    } else {
        Write-Host "  Found: $($pkg.Name)" -ForegroundColor Green
    }
}

if ($missingPackages.Count -gt 0) {
    Write-Host ""
    Write-Host "Installing missing packages..." -ForegroundColor Yellow
    $packagesToInstall = $missingPackages -join " "
    python -m pip install $packagesToInstall
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to install required packages" -ForegroundColor Red
        Write-Host "Try running: pip install flask flask-cors pandas plotly" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "Dependencies installed successfully!" -ForegroundColor Green
}

# Check if ports are in use
Write-Host ""
Write-Host "Checking port availability..." -ForegroundColor Yellow

function Test-PortAvailable {
    param($port)
    try {
        $connections = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
        return -not $connections
    } catch {
        return $true
    }
}

if (Test-PortAvailable $MainPort) {
    Write-Host "  Main Dashboard port $MainPort is available" -ForegroundColor Green
} else {
    Write-Host "  WARNING: Port $MainPort is in use, Main Dashboard may not start" -ForegroundColor Yellow
}

if (Test-PortAvailable $SurtaxPort) {
    Write-Host "  Surtax Dashboard port $SurtaxPort is available" -ForegroundColor Green
} else {
    Write-Host "  WARNING: Port $SurtaxPort is in use, Surtax Dashboard may not start" -ForegroundColor Yellow
}

# Set PYTHONPATH
$env:PYTHONPATH = $scriptDir

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Starting Both Dashboards..." -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "1. Main Contract Dashboard: http://localhost:$MainPort" -ForegroundColor Cyan
Write-Host "2. Surtax Oversight Dashboard: http://localhost:$SurtaxPort" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop the servers" -ForegroundColor Yellow
Write-Host ""

# Start Surtax Dashboard in background
$surtaxDir = Join-Path $scriptDir "surtax_app"
Write-Host "Starting Surtax Oversight Dashboard (port $SurtaxPort)..." -ForegroundColor Gray
$surtaxJob = Start-Job -ScriptBlock {
    param($dir, $port, $pythonPath)
    $env:PYTHONPATH = $pythonPath
    Set-Location $dir
    python -c "from app import app; app.run(debug=False, host='127.0.0.1', port=$port)"
} -ArgumentList $surtaxDir, $SurtaxPort, $scriptDir

# Auto-open browsers after a short delay
if ($AutoOpenBrowser) {
    $browserJob = Start-Job -ScriptBlock {
        param($mainPort, $surtaxPort)
        Start-Sleep -Seconds 3
        Start-Process "http://localhost:$mainPort"
        Start-Sleep -Seconds 1
        Start-Process "http://localhost:$surtaxPort"
    } -ArgumentList $MainPort, $SurtaxPort
    Write-Host "Browsers will open automatically..." -ForegroundColor Gray
}

# Change to web directory for main dashboard
$webDir = Join-Path $scriptDir "web"
Set-Location $webDir

Write-Host "Starting Main Contract Dashboard (port $MainPort)..." -ForegroundColor Gray
Write-Host ""

# Run the main Flask app (foreground)
$env:FLASK_RUN_PORT = $MainPort
python -c "from app import app; app.run(debug=True, host='127.0.0.1', port=$MainPort)"

# Cleanup background job when main app exits
Stop-Job $surtaxJob -ErrorAction SilentlyContinue
Remove-Job $surtaxJob -ErrorAction SilentlyContinue
