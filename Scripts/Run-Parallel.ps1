param(
  [Parameter(Mandatory=$true)][string]$MasterPath,
  [string]$LogIdentifier = "run-log",                               # unique daily/batch id from Scheduled-Runner
  [string]$LogPath = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo", # (kept for compatibility; not used during run)
  [string]$TraceLogPath = "",
  [string]$SheetName = "",
  [string]$PathColumn = "B",
  [int]$StartRow = 2,
  [int]$EndRow = 0,
  [int]$ThrottleLimit = 5,
  [ValidateRange(1,16)][int]$MachineExcelLimit = 3,
  [ValidateRange(1,86400)][int]$ExcelSlotWaitTimeoutSec = 21600,
  [switch]$FastMode,
  [string]$Batch = "23:00",                                         # e.g. 23:00 (11 PM)
  [string]$DbConn = $env:REPORTLOGS_CONN                            # MySQL connection string
)

# ------------------ load helpers & layout ------------------
$here    = Split-Path -Parent $MyInvocation.MyCommand.Path
$helpers = Join-Path $here 'Shared-Excel-Helpers.ps1'
$traceHelpers = Join-Path $here 'Shared-Trace-Helpers.ps1'
$single  = Join-Path $here 'Refresh-One.ps1'
. $helpers
. $traceHelpers

if ([string]::IsNullOrWhiteSpace($TraceLogPath)) {
  $traceDirectory = if ([IO.Path]::HasExtension($LogPath)) {
    Split-Path -Parent $LogPath
  } else {
    $LogPath
  }
  if ([string]::IsNullOrWhiteSpace($traceDirectory)) { $traceDirectory = $here }
  $traceName = (($LogIdentifier -replace '^run-log_', 'refresh-trace_') + '.log')
  $TraceLogPath = Join-Path $traceDirectory $traceName
}

Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'BATCH_START' -Source 'Dispatcher' -Details "ThrottleLimit=$ThrottleLimit; MachineExcelLimit=$MachineExcelLimit; FastMode=$($FastMode.IsPresent)"

# ------------------ load MySql.Data ------------------
Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_MYSQL_LOAD_START' -Source 'Dispatcher'
try {
  Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
  Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_MYSQL_LOAD_END' -Source 'Dispatcher'
} catch {
  # fallback example path; adjust if needed
  $dllGuess = "C:\Program Files (x86)\MySQL\Connector NET 9.0\MySql.Data.dll"
  if (Test-Path $dllGuess) {
    try {
      Add-Type -Path $dllGuess
      Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_MYSQL_LOAD_END' -Source 'Dispatcher' -Details "Fallback=$dllGuess"
    } catch {
      Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_MYSQL_LOAD_ERROR' -Source 'Dispatcher' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
      throw
    }
  } else {
    Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_MYSQL_LOAD_ERROR' -Source 'Dispatcher' -Level 'ERROR' -Details 'MySql.Data assembly not found'
    throw "MySql.Data not found. Install Connector/NET."
  }
}

if (-not $DbConn) {
  Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'DISPATCHER_INPUT_ERROR' -Source 'Dispatcher' -Level 'ERROR' -Details 'Missing database connection string'
  throw "Run-Parallel.ps1: Missing DB connection string. Pass -DbConn or set REPORTLOGS_CONN."
}

# ------------------ derive accepted RunDate(s) from LogIdentifier (overnight-safe) ------------------
# Expecting format: run-log_YYYY-MM-DD_Batch-N (N may be a split ID such as 1.1)
$BatchLogDate = $null
if ($LogIdentifier -match 'run-log_(\d{4}-\d{2}-\d{2})_Batch-\d+(?:\.\d+)?') { $BatchLogDate = $Matches[1] }
$AcceptedDates = @()
if ($BatchLogDate) {
  $d = [datetime]::ParseExact($BatchLogDate,'yyyy-MM-dd',$null)
  $AcceptedDates = @($d.ToString('yyyy-MM-dd'), $d.AddDays(-1).ToString('yyyy-MM-dd'))
}
# Fallback time window if RunDate parsing fails
$HoursLookback = 18
$CutoffUtc     = (Get-Date).ToUniversalTime().AddHours(-$HoursLookback)

# ------------------ read worklist ------------------
Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'WORKLIST_READ_START' -Source 'Dispatcher'
try {
  $items = Get-PathsFromMaster -MasterPath $MasterPath -SheetName $SheetName -PathColumn $PathColumn -StartRow $StartRow -EndRow $EndRow -MachineExcelLimit $MachineExcelLimit -ExcelSlotWaitTimeoutSec $ExcelSlotWaitTimeoutSec
} catch {
  Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'WORKLIST_READ_ERROR' -Source 'Dispatcher' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
  throw
}
if ($items.Count -eq 0) {
  Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'WORKLIST_READ_ERROR' -Source 'Dispatcher' -Level 'ERROR' -Details 'No file paths found'
  throw "No file paths found in $MasterPath."
}
Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'WORKLIST_READ_END' -Source 'Dispatcher' -Details "Count=$($items.Count); ThrottleLimit=$ThrottleLimit; MachineExcelLimit=$MachineExcelLimit"

# Fully-qualified pwsh (helps under Task Scheduler)
$PwshExe = Join-Path $PSHOME 'pwsh.exe'

# ------------------ DB helpers ------------------
function old_Test-AlreadyOkInDb {
  param(
    [string]$ConnStr,
    [string]$FilePath,      # raw PS path like \\192.168.1.237\...
    [string]$RunDate,       # 'yyyy-MM-dd' (single logical day)
    [string]$Batch          # e.g. '12:00'
  )
  Write-Log "In the Test-Already DB function"
  $sql = @"
SELECT 1
FROM events
WHERE stage='Refresh'
  AND status='OK'
  AND batch = @b
  AND rundate = CAST(@rd AS DATE)
  AND LOWER(REPLACE(file_path, '\\\\', '/')) = LOWER(REPLACE(@f, '\\\\', '/'))
LIMIT 1
"@
  Write-Log "SQL query $sql"
  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $p = $cmd.Parameters
    Write-Log "SQL COmmand :- $cmd.CommandText"
    # Use AddWithValue to avoid MySqlDbType enum issues
    [void]$p.AddWithValue("@b",  $Batch)
    [void]$p.AddWithValue("@rd", [datetime]::ParseExact($RunDate,'yyyy-MM-dd',$null))
    [void]$p.AddWithValue("@f",  $FilePath)
    Write-Log "SQL Parm passing ($cmd.Parameters)"
    $r = $cmd.ExecuteScalar()
    return [bool]$r
  } finally {
    if ($conn.State -ne 'Closed') { $conn.Close() }
    $conn.Dispose()
  }
}


function old_Write-SkipRowToDb {
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
    $null = $p.Add("@m",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value       = $(if ($null -ne $Method) { $Method } else { "" })
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
  $machineExcelLimit = $using:MachineExcelLimit
  $slotWaitTimeoutSec = $using:ExcelSlotWaitTimeoutSec
  $traceHelpers = $using:traceHelpers
  $traceLogPath = $using:TraceLogPath

  . $traceHelpers

  $targetPath = $_.Path
  $method     = $_.Method
  $masterRow  = $_.Row

  function Write-DispatchTrace {
    param(
      [Parameter(Mandatory=$true)][string]$Phase,
      [string]$Details = '',
      [ValidateSet('DEBUG','INFO','WARN','ERROR')][string]$Level = 'INFO'
    )
    Write-RefreshTrace -TraceLogPath $traceLogPath -RunId $runId -Batch $batch -FilePath $targetPath -Phase $Phase -Source 'Dispatcher' -Level $Level -Details $Details
  }

  Write-DispatchTrace -Phase 'ITEM_RECEIVED' -Details "MasterRow=$masterRow; Method=$method"

  # ------------------ DB helpers ------------------
function Test-AlreadyOkInDb {
  param(
    [string]$ConnStr,
    [string]$FilePath,      # raw PS path like \\192.168.1.237\...
    [string]$RunDate,       # 'yyyy-MM-dd' (single logical day)
    [string]$Batch          # e.g. '12:00'
  )
  $sql = @"
SELECT 1
FROM events
WHERE stage='Refresh'
  AND status='OK'
  AND batch = @b
  AND rundate = CAST(@rd AS DATE)
  AND LOWER(REPLACE(file_path, '\\\\', '/')) = LOWER(REPLACE(@f, '\\\\', '/'))
LIMIT 1
"@
  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $p = $cmd.Parameters
    # Use AddWithValue to avoid MySqlDbType enum issues
    [void]$p.AddWithValue("@b",  $Batch)
    [void]$p.AddWithValue("@rd", [datetime]::ParseExact($RunDate,'yyyy-MM-dd',$null))
    [void]$p.AddWithValue("@f",  $FilePath)
    $r = $cmd.ExecuteScalar()
    return [bool]$r
  } finally {
    if ($conn.State -ne 'Closed') { $conn.Close() }
    $conn.Dispose()
  }
}

function Write-SkipRowToDb {
  param(
    [string]$ConnStr,
    [string]$RunId,
    [string]$RunDate,   # yyyy-MM-dd
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
(@run,@batch,'Refresh',@ts,CAST(@rd AS DATE),@mp,@fp,@m,'SKIP','Already OK for this batch/day',0,NULL,NULL)
"@

  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql

    # No MySqlDbType enums here (avoids CLS casing issue)
    [void]$cmd.Parameters.AddWithValue("@run",  $RunId)
    [void]$cmd.Parameters.AddWithValue("@batch",$Batch)
    [void]$cmd.Parameters.AddWithValue("@ts",   $utc)                                        # DATETIME
    [void]$cmd.Parameters.AddWithValue("@rd",   [datetime]::ParseExact($RunDate,'yyyy-MM-dd',$null)) # DATE via CAST
    [void]$cmd.Parameters.AddWithValue("@mp",   ($(if ($MasterPath) { $MasterPath } else { [DBNull]::Value })))
    [void]$cmd.Parameters.AddWithValue("@fp",   $FilePath)
    [void]$cmd.Parameters.AddWithValue("@m",    ($(if ($Method) { $Method } else { "" })))

    [void]$cmd.ExecuteNonQuery()
  } finally {
    if ($conn.State -ne 'Closed') { $conn.Close() }
    $conn.Dispose()
  }
}

  # --- DB-based skip check ---
  $alreadyOk = $false
  Write-DispatchTrace -Phase 'DB_SKIP_CHECK_START' -Details "RunDate=$logRunDate"
  try {
    $alreadyOk = Test-AlreadyOkInDb -ConnStr $connStr -FilePath $targetPath -Batch $batch -RunDate $logRunDate
    Write-DispatchTrace -Phase 'DB_SKIP_CHECK_END' -Details "AlreadyOk=$alreadyOk"
  } catch {
    # If DB is down, be safe and DO NOT skip (process the file)
    $alreadyOk = $false
    Write-DispatchTrace -Phase 'DB_SKIP_CHECK_ERROR' -Level 'WARN' -Details "ContinuingWithRefresh=True; Error=$($_.Exception.Message)"
  }
  if ($alreadyOk) {
    # Write SKIP event to DB (so final CSV export shows it too)
    Write-DispatchTrace -Phase 'ITEM_SKIP_START' -Details 'Reason=Already OK for this batch/day'
    try {
      try { Add-Type -AssemblyName "MySql.Data" -ErrorAction SilentlyContinue } catch {Write-Host "Error to load"}
      Write-SkipRowToDb -ConnStr $connStr -RunId $runId -RunDate $logRunDate -Batch $batch -MasterPath $master -FilePath $targetPath -Method $method
      Write-DispatchTrace -Phase 'ITEM_SKIP_END' -Details 'DatabaseEvent=Written'
    } catch { 
      Write-Host "in the catch block of skip write to DB"
      Write-DispatchTrace -Phase 'ITEM_SKIP_LOG_ERROR' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
    }
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
    '-DbConn',  $connStr,         # pass DB conn
    '-TraceLogPath', $traceLogPath,
    '-MachineExcelLimit', $machineExcelLimit,
    '-ExcelSlotWaitTimeoutSec', $slotWaitTimeoutSec
  )
  if ($fastFlag) { $args += '-FastMode' }

  Write-DispatchTrace -Phase 'WORKER_LAUNCH' -Details "MasterRow=$masterRow; Method=$method"
  & $Pwsh @args
  $workerExitCode = $LASTEXITCODE
  $workerExitLevel = if ($workerExitCode -eq 0) { 'INFO' } else { 'ERROR' }
  Write-DispatchTrace -Phase 'WORKER_EXIT' -Level $workerExitLevel -Details "ExitCode=$workerExitCode"
} -ThrottleLimit $ThrottleLimit

Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $MasterPath -Phase 'BATCH_DISPATCH_COMPLETE' -Source 'Dispatcher' -Details "Count=$($items.Count)"
