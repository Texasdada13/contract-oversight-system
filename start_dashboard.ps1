# Contract Oversight System - Startup Script
# Run this script to start the web dashboard

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Contract Oversight System" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Get script directory
$scriptDir = $PSScriptRoot
if (-not $scriptDir) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}

# Configuration
$Port = 5002
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

# Check if port is in use
Write-Host ""
Write-Host "Checking port $Port availability..." -ForegroundColor Yellow

$portInUse = $false
try {
    $connections = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($connections) {
        $portInUse = $true
    }
} catch {
    # Port check failed, assume available
}

if ($portInUse) {
    Write-Host "WARNING: Port $Port is already in use!" -ForegroundColor Red
    Write-Host ""

    # Try to find an alternative port
    $alternativePorts = @(5003, 5004, 5005, 5006, 8080, 8081)
    $foundPort = $false

    foreach ($altPort in $alternativePorts) {
        try {
            $altConnections = Get-NetTCPConnection -LocalPort $altPort -ErrorAction SilentlyContinue
            if (-not $altConnections) {
                $Port = $altPort
                $foundPort = $true
                Write-Host "Using alternative port: $Port" -ForegroundColor Yellow
                break
            }
        } catch {
            $Port = $altPort
            $foundPort = $true
            Write-Host "Using alternative port: $Port" -ForegroundColor Yellow
            break
        }
    }

    if (-not $foundPort) {
        Write-Host "ERROR: Could not find an available port" -ForegroundColor Red
        Write-Host "Please close the application using port $Port and try again" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host "Port $Port is available" -ForegroundColor Green

# Set PYTHONPATH
$env:PYTHONPATH = $scriptDir

# Change to web directory
$webDir = Join-Path $scriptDir "web"
Set-Location $webDir

Write-Host ""
Write-Host "Starting Contract Oversight Dashboard..." -ForegroundColor Green
Write-Host "Dashboard will be available at: http://localhost:$Port" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
Write-Host ""

# Auto-open browser after a short delay
if ($AutoOpenBrowser) {
    $job = Start-Job -ScriptBlock {
        param($port)
        Start-Sleep -Seconds 2
        Start-Process "http://localhost:$port"
    } -ArgumentList $Port
    Write-Host "Browser will open automatically..." -ForegroundColor Gray
}

# Run the Flask app with the selected port
$env:FLASK_RUN_PORT = $Port
python -c "from app import app; app.run(debug=True, host='127.0.0.1', port=$Port)"
