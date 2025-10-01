param(
  [Parameter(Mandatory=$true)][string]$MasterPath,
  [string]$LogPath   = "C:\ReportRunner\run-log.csv",
  [string]$SheetName = "",
  [string]$PathColumn= "B",
  [int]$StartRow     = 2,
  [int]$EndRow       = 0,
  [int]$ThrottleLimit = 5,
  [switch]$FastMode,
  [string]$Batch = "1:00"   # e.g., "01:00" or "Nightly"
)

$here    = Split-Path -Parent $MyInvocation.MyCommand.Path
$helpers = Join-Path $here 'Shared-Excel-Helpers.ps1'
$single  = Join-Path $here 'Refresh-One.ps1'

. $helpers

$items = Get-PathsFromMaster -MasterPath $MasterPath -SheetName $SheetName -PathColumn $PathColumn -StartRow $StartRow -EndRow $EndRow
if ($items.Count -eq 0) { throw "No file paths found in $MasterPath." }

# Ensure log dir exists
New-Item -ItemType Directory -Force -Path (Split-Path $LogPath) | Out-Null

$items | ForEach-Object -Parallel {
  $single = $using:single
  $log    = $using:LogPath
  $master = $using:MasterPath
  $fast   = if ($using:FastMode) { '-FastMode' } else { '' }
  $batch  = $using:Batch

  $args = @(
    '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass',
    '-File', $single,
    '-Path', $_.Path,
    '-Method', $_.Method,
    '-Master', $master,
    '-Batch',  $batch,
    '-LogPath', $log
  )
  if ($fast -ne '') { $args += $fast }

  & pwsh @args
} -ThrottleLimit $ThrottleLimit
