# ops/auto_commit_push.ps1
param(
  [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"

# Go to repo root (this script sits in ops/)
Set-Location -Path (Split-Path $PSScriptRoot -Parent)

# Optional: use SSH for non-interactive push (recommended)
# git remote set-url origin git@github.com:abhishek1729-sys/Intraday.git

# Pull latest
git fetch origin $Branch
git pull --rebase origin $Branch

# Stage everything that's not ignored
git add -A

# If nothing to commit, exit cleanly
$changed = git diff --cached --name-only
if (-not $changed) {
  Write-Output "[AUTO] Nothing to commit."
  exit 0
}

# Commit with timestamp
$ts = (Get-Date).ToString("yyyy-MM-dd HH:mm")
$hostName = $env:COMPUTERNAME
$commitMsg = "Auto EOD commit $ts ($hostName)"

git commit -m $commitMsg

# Push
git push origin $Branch

Write-Output "[AUTO] Commit & push done: $commitMsg"
