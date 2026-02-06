#!/bin/bash
# Navigate to the project directory
cd /home/omar/Documents/Dust/iraqairquality

# Add all changes
git add .

# Commit with a timestamp (e.g., "Auto-commit: 2026-02-06 16:45")
git commit -m "Auto-update: $(date +'%d%b%H%M')"

# Push to the main branch
git push origin main
