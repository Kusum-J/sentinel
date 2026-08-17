# Sentinel Seed Script for Windows PowerShell
# This script adds a working URL and a broken URL to the monitor list, then lists them.

$API_URL = "http://localhost:8080/urls"

Write-Host "----------------------------------------" -ForegroundColor Cyan
Write-Host "Seeding Sentinel Monitor List" -ForegroundColor Cyan
Write-Host "----------------------------------------" -ForegroundColor Cyan

# 1. Add a working URL (checks every 10s)
Write-Host "1. Adding working URL (https://httpbin.org/status/200)..." -NoNewline
$workingPayload = @{
    url = "https://httpbin.org/status/200"
    interval_seconds = 10
} | ConvertTo-Json

try {
    $workingRes = Invoke-RestMethod -Uri $API_URL -Method Post -Body $workingPayload -ContentType "application/json"
    Write-Host " [SUCCESS]" -ForegroundColor Green
    Write-Host "   ID: $($workingRes.id)"
} catch {
    Write-Host " [FAILED]" -ForegroundColor Red
    Write-Host "   Error: $_"
}

# 2. Add a broken URL (checks every 10s)
Write-Host "2. Adding broken URL (http://localhost:9999)..." -NoNewline
$brokenPayload = @{
    url = "http://localhost:9999"
    interval_seconds = 10
} | ConvertTo-Json

try {
    $brokenRes = Invoke-RestMethod -Uri $API_URL -Method Post -Body $brokenPayload -ContentType "application/json"
    Write-Host " [SUCCESS]" -ForegroundColor Green
    Write-Host "   ID: $($brokenRes.id)"
} catch {
    Write-Host " [FAILED]" -ForegroundColor Red
    Write-Host "   Error: $_"
}

Write-Host ""
Write-Host "3. Listing all monitors to verify:" -ForegroundColor Cyan
try {
    $list = Invoke-RestMethod -Uri $API_URL -Method Get
    $list | ConvertTo-Json -Depth 5
} catch {
    Write-Host "Failed to list monitors. Make sure the API server is running." -ForegroundColor Red
}
