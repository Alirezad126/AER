param(
  [Parameter(Mandatory=$true)][int]$Minutes,
  [string]$Dashboards = "all",
  [int]$Workers = 2,
  [switch]$Headless,
  [switch]$Force,
  [switch]$PushToS3,
  [switch]$PurgeLocal,
  [switch]$PurgeWell,
  [switch]$CheckRemote,
  [int]$Timeout = 40,
  [double]$Delay = 0.2,
  [string]$Wells = "wells.txt"
)

$end = (Get-Date).AddMinutes($Minutes)
Write-Host "[info] End time: $end"

while ((Get-Date) -lt $end) {
  $args = @(
    "aer_multi_dash_mp.py",
    "--workers", $Workers,
    "--wells", $Wells,
    "--dashboards", $Dashboards,
    "--sheets", "all",
    "--timeout", $Timeout,
    "--delay", $Delay
  )
  if ($Headless)    { $args += "--headless" }
  if ($Force)       { $args += "--force" }
  if ($PushToS3)    { $args += "--push-to-s3" }
  if ($PurgeLocal)  { $args += "--purge-local" }
  if ($PurgeWell)   { $args += "--purge-well" }
  if ($CheckRemote) { $args += "--check-remote" }

  Write-Host "[info] Launching: python $($args -join ' ')"
  & python @args
  $code = $LASTEXITCODE
  Write-Host "[info] Scraper exited with code $code"

  if ((Get-Date) -ge $end) { break }
  Start-Sleep -Seconds 2
}

Write-Host "[done] Scrape window complete."
