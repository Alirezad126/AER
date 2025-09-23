[CmdletBinding()]
param(
  [string]$OutBase = 'Data',
  [string]$Remote  = 'aer:aer-scrape-prod',
  [switch]$AllData,
  [switch]$DryRun
)

$rclone = Get-Command rclone -ErrorAction SilentlyContinue
if (-not $rclone) { Write-Error 'rclone not found on PATH'; exit 1 }

if (-not (Test-Path $OutBase)) { New-Item -ItemType Directory -Path $OutBase | Out-Null }

$dst = (Resolve-Path $OutBase).Path
$src = "$Remote/Data"

if ($AllData) {
  $args = @('sync', $src, $dst, '--size-only', '--exclude', '_tmp_worker_*', '--exclude', 'locks/**')
} else {
  # Manifests only (fast & tiny)
  $args = @('copy', $src, $dst, '--include', '*/sheets.txt', '--max-depth', '3')
}

if ($DryRun) { $args = @('--dry-run') + $args }

Write-Host ("[info] rclone {0}" -f ($args -join ' '))
& $rclone.Source @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host '[done] Sync down complete.'
