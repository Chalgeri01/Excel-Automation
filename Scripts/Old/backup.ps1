param(
  [Parameter(Mandatory=$true)][string]$MasterPath,
  [string]$LogIdentifier = "run-log", # Accepts the unique log identifier from Scheduled-Runner
  [string]$LogPath = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo", # Log directory
  [string]$SheetName = "",
  [string]$PathColumn = "B",
  [int]$StartRow = 2,
  [int]$EndRow = 0,
  [int]$ThrottleLimit = 5,
  [switch]$FastMode,
  [string]$Batch = "23:00"   # The batch time, e.g., 23:00 for 11 PM
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$helpers = Join-Path $here 'Shared-Excel-Helpers.ps1'
$single = Join-Path $here 'Refresh-One.ps1'

. $helpers

# --- NEW: Use the LogIdentifier parameter to create the log file path ---
$LogDirectory = $LogPath
$DailyLogPath = Join-Path $LogDirectory "$LogIdentifier.csv"

# Ensure log directory exists
New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null


$items = Get-PathsFromMaster -MasterPath $MasterPath -SheetName $SheetName -PathColumn $PathColumn -StartRow $StartRow -EndRow $EndRow
if ($items.Count -eq 0) { throw "No file paths found in $MasterPath." }

# Pass the calculated $DailyLogPath to the parallel jobs
$items | ForEach-Object -Parallel {
  $single = $using:single
  $log = $using:DailyLogPath # Use the daily log path with the unique identifier
  $master = $using:MasterPath
  $fast = if ($using:FastMode) { '-FastMode' } else { '' }
  $batch = $using:Batch

  $args = @(
    '-NoLogo', '-NoProfile', '-ExecutionPolicy', 'Bypass',
    '-File', $single,
    '-Path', $_.Path,
    '-Method', $_.Method,
    '-Master', $master,
    '-Batch', $batch,
    '-LogPath', $log # Pass the daily log path to the child script
  )
  if ($fast -ne '') { $args += $fast }

  & pwsh @args
} -ThrottleLimit $ThrottleLimit