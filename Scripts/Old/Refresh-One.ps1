param(
  [Parameter(Mandatory=$true)][string]$Path,
  [string]$LogPath = "C:\ReportRunner\run-log.csv",
  [int]$TimeoutSec = 900,
  [switch]$FastMode,
  [string]$Master = "",
  [string]$Method = "",
  [string]$Batch  = ""
)

. "$PSScriptRoot\Shared-Excel-Helpers.ps1"

# Ensure log dir
$logDir = Split-Path $LogPath -Parent
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force -Path $logDir | Out-Null }

# Refresh
$excel = Start-Excel
$status = "OK"; $err = ""; $t0 = Get-Date
try {
  Refresh-WorkbookSmart -excel $excel -Path $Path -TimeoutSec $TimeoutSec -FastMode:$FastMode
} catch {
  $status = "FAIL"; $err = $_.Exception.Message
} finally {
  Stop-Excel $excel
}

# Build log row (Stage = Refresh)
$now     = Get-Date
$rundate = $now.ToString("yyyy-MM-dd")
$row = [pscustomobject]@{
  Timestamp    = $now.ToString("s")
  RunDate      = $rundate
  Batch        = $Batch
  Stage        = "Refresh"
  Master       = $Master
  FilePath     = $Path
  Method       = $Method
  Status       = $status
  Error        = $err
  DurationS    = [int]((Get-Date) - $t0).TotalSeconds
  RecipientsTo = ""
  Subject      = ""
}

# Append/create with stable schema
if (Test-Path $LogPath) {
  $row | Export-Csv -Path $LogPath -NoTypeInformation -Append -Encoding UTF8
} else {
  $row | Export-Csv -Path $LogPath -NoTypeInformation -Encoding UTF8
}
