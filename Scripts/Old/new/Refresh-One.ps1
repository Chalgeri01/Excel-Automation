param(
  [Parameter(Mandatory=$true)][string]$Path,
  [string]$LogPath = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\run-log.csv",
  [int]$TimeoutSec = 900,
  [switch]$FastMode,
  [string]$Master = ""   
)

. "$PSScriptRoot\Shared-Excel-Helpers.ps1"

# Ensure log folder exists
$logDir = Split-Path $LogPath -Parent
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force -Path $logDir | Out-Null }

$excel = Start-Excel
$status = "OK"; $err = ""; $t0 = Get-Date
try {
  Refresh-WorkbookSmart -excel $excel -Path $Path -TimeoutSec $TimeoutSec -FastMode:$FastMode
} catch {
  $status = "FAIL"; $err = $_.Exception.Message
} finally {
  Stop-Excel $excel
}

# ALWAYS log the same columns (include Master)
$obj = [pscustomobject]@{
  Timestamp = (Get-Date).ToString("s")
  Master    = $Master         
  FilePath  = $Path
  Status    = $status
  Error     = $err
  DurationS = [int]((Get-Date) - $t0).TotalSeconds
}

# Append safely; create with headers on first write
if (Test-Path $LogPath) {
  $obj | Export-Csv -Path $LogPath -NoTypeInformation -Append -Encoding UTF8
} else {
  $obj | Export-Csv -Path $LogPath -NoTypeInformation -Encoding UTF8
}
