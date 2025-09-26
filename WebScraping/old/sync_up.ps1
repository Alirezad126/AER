[CmdletBinding()]
param(
  [string]$OutBase = 'Data',
  [string]$Remote  = 'aer:aer-scrape-prod',
  [switch]$DryRun
)

# rclone required
$rclone = Get-Command rclone -ErrorAction SilentlyContinue
if (-not $rclone) { Write-Error 'rclone not found on PATH'; exit 1 }

if (-not (Test-Path $OutBase)) {
  Write-Host "[info] Nothing to upload; '$OutBase' does not exist."
  exit 0
}

$src = (Resolve-Path $OutBase).Path
$dst = "$Remote/Data"

# Exclude temp worker dirs, lock area, and OS junk
$common = @(
  'copy',
  $src, $dst,
  '--ignore-existing',
  '--create-empty-src-dirs',
  '--exclude', '_tmp_worker_*',
  '--exclude', 'locks/**',
  '--exclude', '.DS_Store',
  '--exclude', 'Thumbs.db'
)

if ($DryRun) { $common = @('--dry-run') + $common }

Write-Host ("[info] rclone {0}" -f ($common -join ' '))
& $rclone.Source @common
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host '[done] Sync up complete.'
