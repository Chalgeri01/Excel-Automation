param(
  # Master workbook with list of full paths (column/row settings below)
  [Parameter(Mandatory=$true)][string]$MasterPath,

  # Where the parallel runs will append their logs
  [string]$LogPath   = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\run-log.csv",

  # Sheet/column/range in master
  [string]$SheetName = "",
  [string]$PathColumn= "B",
  [int]$StartRow     = 2,
  [int]$EndRow       = 0,

  # Parallelism
  [int]$ThrottleLimit = 5,

  # Pass FastMode to single runners for speed
  [switch]$FastMode
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$helpers = Join-Path $here 'Shared-Excel-Helpers.ps1'
$single  = Join-Path $here 'Refresh-One.ps1'

. $helpers

# Get list of report paths from the master workbook
$paths = Get-PathsFromMaster -MasterPath $MasterPath -SheetName $SheetName -PathColumn $PathColumn -StartRow $StartRow -EndRow $EndRow
if ($paths.Count -eq 0) { throw "No file paths found in $MasterPath (column $PathColumn, row $StartRow..$EndRow)." }

# Ensure log dir
New-Item -ItemType Directory -Force -Path (Split-Path $LogPath) | Out-Null

# Fan-out: each parallel item launches an isolated pwsh process to avoid COM cross-thread issues
$paths | ForEach-Object -Parallel {
  $single     = $using:single
  $log        = $using:LogPath
  $master     = $using:MasterPath
  $fast       = if ($using:FastMode) { '-FastMode' } else { '' }
  $args = @(
    '-NoLogo','-NoProfile',
    '-File', $single,
    '-Path', $_,
    '-LogPath', $log
    '-Master', $master
  )
  if ($fast -ne '') { $args += $fast }
  # Launch a separate Excel per file (isolated process)
  & pwsh @args
} -ThrottleLimit $ThrottleLimit
