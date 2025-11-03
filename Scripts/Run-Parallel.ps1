param(
  [Parameter(Mandatory=$true)][string]$MasterPath,
  [string]$LogIdentifier = "run-log",                               # unique daily/batch id from Scheduled-Runner
  [string]$LogPath = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo", # (kept for compatibility; not used during run)
  [string]$SheetName = "",
  [string]$PathColumn = "B",
  [int]$StartRow = 2,
  [int]$EndRow = 0,
  [int]$ThrottleLimit = 5,
  [switch]$FastMode,
  [string]$Batch = "23:00",                                         # e.g. 23:00 (11 PM)
  [string]$DbConn = $env:REPORTLOGS_CONN                            # MySQL connection string
)

# ------------------ load helpers & layout ------------------
$here    = Split-Path -Parent $MyInvocation.MyCommand.Path
$helpers = Join-Path $here 'Shared-Excel-Helpers.ps1'
$single  = Join-Path $here 'Refresh-One.ps1'
. $helpers

# ------------------ load MySql.Data ------------------
try {
  Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
} catch {
  # fallback example path; adjust if needed
  $dllGuess = "C:\Program Files (x86)\MySQL\Connector NET 9.0\MySql.Data.dll"
  if (Test-Path $dllGuess) { Add-Type -Path $dllGuess } else { throw "MySql.Data not found. Install Connector/NET." }
}

if (-not $DbConn) { throw "Run-Parallel.ps1: Missing DB connection string. Pass -DbConn or set REPORTLOGS_CONN." }

# ------------------ derive accepted RunDate(s) from LogIdentifier (overnight-safe) ------------------
# Expecting format: run-log_YYYY-MM-DD_Batch-N
$BatchLogDate = $null
if ($LogIdentifier -match 'run-log_(\d{4}-\d{2}-\d{2})_Batch-\d+') { $BatchLogDate = $Matches[1] }
$AcceptedDates = @()
if ($BatchLogDate) {
  $d = [datetime]::ParseExact($BatchLogDate,'yyyy-MM-dd',$null)
  $AcceptedDates = @($d.ToString('yyyy-MM-dd'), $d.AddDays(-1).ToString('yyyy-MM-dd'))
}
# Fallback time window if RunDate parsing fails
$HoursLookback = 18
$CutoffUtc     = (Get-Date).ToUniversalTime().AddHours(-$HoursLookback)

# ------------------ read worklist ------------------
$items = Get-PathsFromMaster -MasterPath $MasterPath -SheetName $SheetName -PathColumn $PathColumn -StartRow $StartRow -EndRow $EndRow
if ($items.Count -eq 0) { throw "No file paths found in $MasterPath." }

# Fully-qualified pwsh (helps under Task Scheduler)
$PwshExe = Join-Path $PSHOME 'pwsh.exe'

# ------------------ DB helpers ------------------
function Test-AlreadyOkInDb {
  param(
    [string]$ConnStr,
    [string]$FilePath,
    [string[]]$AcceptRunDates,
    [datetime]$CutoffUtc,
    [string]$Batch
  )
  # 1) try RunDate match (fast exact)
  if ($AcceptRunDates -and $AcceptRunDates.Count -gt 0) {
    $q1 = @"
SELECT 1
FROM events
WHERE LOWER(REPLACE(file_path, '\\\\', '\\')) = LOWER(@f)
  AND stage = 'Refresh'
  AND status = 'OK'
  AND batch = @b
  AND rundate IN (@d1{0})
LIMIT 1
"@
    # build IN list params dynamically
    write-host "SQL query"
    write-host $q1
    $inParams = @()
    for ($i=0; $i -lt $AcceptRunDates.Count; $i++) { $inParams += ",@d$($i+1)" }
    $sql = [string]::Format($q1, ($inParams -join ''))
    write-host $sql
    $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
    try {
      $conn.Open()
      $cmd = $conn.CreateCommand()
      $cmd.CommandText = $sql
      $p = $cmd.Parameters
      $null = $p.Add("@f",[MySql.Data.MySqlClient.MySqlDbType]::LongText).Value = $FilePath
      $null = $p.Add("@b",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value  = $Batch
      for ($i=0; $i -lt $AcceptRunDates.Count; $i++) {
        $null = $p.Add("@d$($i+1)",[MySql.Data.MySqlClient.MySqlDbType]::Date).Value = [datetime]::ParseExact($AcceptRunDates[$i],'yyyy-MM-dd',$null)
      }
      $r = $cmd.ExecuteScalar()
      if ($r) { return $true }
    } finally { $conn.Close(); $conn.Dispose() }
  }

  # 2) fallback: any OK since cutoff UTC (covers partial-overnight edge)
  $q2 = @"
SELECT 1
FROM events
WHERE LOWER(REPLACE(file_path, '\\\\', '\\')) = LOWER(@f)
  AND stage = 'Refresh'
  AND status = 'OK'
  AND batch = @b
  AND timestamp_utc >= @cut
LIMIT 1
"@
  $conn2 = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    $conn2.Open()
    $cmd2 = $conn2.CreateCommand()
    $cmd2.CommandText = $q2
    $p2 = $cmd2.Parameters
    $null = $p2.Add("@f",[MySql.Data.MySqlClient.MySqlDbType]::LongText).Value   = $FilePath
    $null = $p2.Add("@b",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value    = $Batch
    $null = $p2.Add("@cut",[MySql.Data.MySqlClient.MySqlDbType]::DateTime).Value = $CutoffUtc
    $r2 = $cmd2.ExecuteScalar()
    return [bool]$r2
  } finally { $conn2.Close(); $conn2.Dispose() }
}

function Write-SkipRowToDb {
  param(
    [string]$ConnStr,
    [string]$RunId,
    [string]$RunDate,         # yyyy-MM-dd (logical date)
    [string]$Batch,
    [string]$MasterPath,
    [string]$FilePath,
    [string]$Method
  )
  $utc = (Get-Date).ToUniversalTime()
  $sql = @"
INSERT INTO events
(run_id,batch,stage,timestamp_utc,rundate,master_path,file_path,method,status,error_text,duration_s,recipients_to,subject)
VALUES
(@run,@batch,'Refresh',@ts,@rd,@mp,@fp,@m,'SKIP','Already OK for this batch/day',0,NULL,NULL)
"@
  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $p = $cmd.Parameters
    $null = $p.Add("@run",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value     = $RunId
    $null = $p.Add("@batch",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = $Batch
    $null = $p.Add("@ts",[MySql.Data.MySqlClient.MySqlDbType]::DateTime).Value     = $utc
    $null = $p.Add("@rd",[MySql.Data.MySqlClient.MySqlDbType]::Date).Value         = [datetime]::ParseExact($RunDate,'yyyy-MM-dd',$null)
    $null = $p.Add("@mp",[MySql.Data.MySqlClient.MySqlDbType]::LongText).Value     = $MasterPath
    $null = $p.Add("@fp",[MySql.Data.MySqlClient.MySqlDbType]::LongText).Value     = $FilePath
    $null = $p.Add("@m",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value       = ($Method ?? "")
    [void]$cmd.ExecuteNonQuery()
  } finally { $conn.Close(); $conn.Dispose() }
}

# Determine the logical RunDate to stamp for SKIP/forwarding
# Prefer the parsed batch date; fall back to today.
$LogicalRunDate = if ($BatchLogDate) { $BatchLogDate } else { (Get-Date).ToString('yyyy-MM-dd') }

# ------------------ fan-out with DB-backed skip check ------------------
$items | ForEach-Object -Parallel {
  $single    = $using:single
  $master    = $using:MasterPath
  $fastFlag  = $using:FastMode
  $batch     = $using:Batch
  $Pwsh      = $using:PwshExe
  $accDates  = $using:AcceptedDates
  $cutoff    = $using:CutoffUtc
  $connStr   = $using:DbConn
  $runId     = $using:LogIdentifier
  $logRunDate= $using:LogicalRunDate

  $targetPath = $_.Path
  $method     = $_.Method

  # --- DB-based skip check ---
  $alreadyOk = $false
  try {
    $alreadyOk = Test-AlreadyOkInDb -ConnStr $connStr -FilePath $targetPath -AcceptRunDates $accDates -CutoffUtc $cutoff -Batch $batch
  } catch {
    # If DB is down, be safe and DO NOT skip (process the file)
    $alreadyOk = $false
  }

  if ($alreadyOk) {
    # Write SKIP event to DB (so final CSV export shows it too)
    try {
      Write-SkipRowToDb -ConnStr $connStr -RunId $runId -RunDate $logRunDate -Batch $batch -MasterPath $master -FilePath $targetPath -Method $method
    } catch { }
    return
  }

  # --- Not already OK: call the per-file refresher (will log OK/FAIL to DB) ---
  $args = @(
    '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass',
    '-File', $single,
    '-Path',   $targetPath,
    '-Method', $method,
    '-Master', $master,
    '-Batch',  $batch,
    '-LogIdentifier', $runId,     # pass run_id
    '-RunDate', $logRunDate,      # pass logical date
    '-DbConn',  $connStr          # pass DB conn
  )
  if ($fastFlag) { $args += '-FastMode' }

  & $Pwsh @args
} -ThrottleLimit $ThrottleLimit