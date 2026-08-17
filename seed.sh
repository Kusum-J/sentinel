#!/bin/bash
# Sentinel Seed Script for Unix/macOS/Git Bash
# This script adds a working URL and a broken URL to the monitor list, then lists them.

API_URL="http://localhost:8080/urls"

echo "----------------------------------------"
echo "Seeding Sentinel Monitor List"
echo "----------------------------------------"

# 1. Add a working URL (checks every 10s)
echo "1. Adding working URL (https://httpbin.org/status/200)..."
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"url":"https://httpbin.org/status/200", "interval_seconds":10}' \
  $API_URL
echo -e "\n"

# 2. Add a broken URL (checks every 10s)
echo "2. Adding broken URL (http://localhost:9999)..."
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"url":"http://localhost:9999", "interval_seconds":10}' \
  $API_URL
echo -e "\n"

# 3. List monitors
echo "3. Listing all monitors:"
curl -s $API_URL | json_pp || curl -s $API_URL
echo -e "\n"
