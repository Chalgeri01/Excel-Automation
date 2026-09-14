# Refresh-One.ps1 — DB-backed single workbook refresh + event write
param(
  [Parameter(Mandatory = $true)][string]$Path,
  [int]$TimeoutSec = 900,
  [ValidateRange(1,16)][int]$MachineExcelLimit = 3,
  [ValidateRange(1,86400)][int]$ExcelSlotWaitTimeoutSec = 21600,
  [switch]$FastMode,
  [string]$Master = "",
  [string]$Method = "",
  [string]$Batch  = "",
  # --- DB logging (required in DB mode) ---
  [Parameter(Mandatory = $true)][string]$DbConn,
  [Parameter(Mandatory = $true)][string]$LogIdentifier,   # e.g. run-log_YYYY-MM-DD_Batch-1
  [string]$RunDate = "",                                   # yyyy-MM-dd (logical day for this run)
  [string]$TraceLogPath = ""
)

. "$PSScriptRoot\Shared-Trace-Helpers.ps1"
. "$PSScriptRoot\Shared-Excel-Helpers.ps1"

if ([string]::IsNullOrWhiteSpace($TraceLogPath)) {
  $traceDirectory = Join-Path (Split-Path -Parent $PSScriptRoot) 'Logginfo'
  $traceName = (($LogIdentifier -replace '^run-log_', 'refresh-trace_') + '.log')
  $TraceLogPath = Join-Path $traceDirectory $traceName
}

$script:RefreshTraceStart = Get-Date
$script:RefreshTraceExcelPid = $null

function Write-WorkerTrace {
  param(
    [Parameter(Mandatory=$true)][string]$Phase,
    [string]$Details = '',
    [ValidateSet('DEBUG','INFO','WARN','ERROR')][string]$Level = 'INFO',
    [AllowNull()][object]$ExcelPid = $null,
    [string]$Source = 'Worker'
  )

  if ($null -ne $ExcelPid -and [string]$ExcelPid -ne '') {
    $script:RefreshTraceExcelPid = [int]$ExcelPid
  }
  $elapsedSeconds = ((Get-Date) - $script:RefreshTraceStart).TotalSeconds
  Write-RefreshTrace -TraceLogPath $TraceLogPath -RunId $LogIdentifier -Batch $Batch -FilePath $Path -Phase $Phase -Level $Level -Source $Source -Details $Details -ExcelPid $script:RefreshTraceExcelPid -ElapsedSeconds $elapsedSeconds
}

$traceCallback = {
  param($Phase, $Details = '', $Level = 'INFO', $ExcelPid = $null, $Source = 'Excel')
  Write-WorkerTrace -Phase $Phase -Details $Details -Level $Level -ExcelPid $ExcelPid -Source $Source
}

Write-WorkerTrace -Phase 'PROCESS_START' -Details "Method=$Method; TimeoutSec=$TimeoutSec; MachineExcelLimit=$MachineExcelLimit; FastMode=$($FastMode.IsPresent)"

# ---------- Load MySql.Data ----------
function Load-MySqlAssembly {
  try {
    Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
    return
  } catch {
    # Fallback paths — adjust to your installed Connector/NET version if needed
    $fallbacks = @(
      "C:\Program Files (x86)\MySQL\Connector NET 9.4\MySql.Data.dll",
      "C:\Program Files (x86)\MySQL\Connector NET 9.0\MySql.Data.dll",
      "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.4\MySql.Data.dll"
    )
    foreach ($dll in $fallbacks) {
      if (Test-Path $dll) {
        Add-Type -Path $dll
        return
      }
    }
    throw "MySql.Data not found. Install MySQL Connector/NET or update fallback paths in Refresh-One.ps1."
  }
}
Write-WorkerTrace -Phase 'MYSQL_ASSEMBLY_LOAD_START'
try {
  Load-MySqlAssembly
  Write-WorkerTrace -Phase 'MYSQL_ASSEMBLY_LOAD_END'
} catch {
  Write-WorkerTrace -Phase 'MYSQL_ASSEMBLY_LOAD_ERROR' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
  throw
}

# ---------- Param validation / normalization ----------
Write-WorkerTrace -Phase 'INPUT_VALIDATION_START'
if (-not (Test-Path -LiteralPath $Path)) {
  Write-WorkerTrace -Phase 'INPUT_VALIDATION_ERROR' -Level 'ERROR' -Details 'File not found'
  throw "Refresh-One.ps1: file not found: $Path"
}
if ([string]::IsNullOrWhiteSpace($DbConn)) {
  Write-WorkerTrace -Phase 'INPUT_VALIDATION_ERROR' -Level 'ERROR' -Details 'Missing database connection string'
  throw "Refresh-One.ps1: Missing -DbConn (MySQL connection string)."
}
if ([string]::IsNullOrWhiteSpace($LogIdentifier)) {
  Write-WorkerTrace -Phase 'INPUT_VALIDATION_ERROR' -Level 'ERROR' -Details 'Missing run identifier'
  throw "Refresh-One.ps1: Missing -LogIdentifier (run_id)."
}

# Normalize RunDate (logical local day) → yyyy-MM-dd
if ([string]::IsNullOrWhiteSpace($RunDate)) {
  $RunDate = (Get-Date).ToString('yyyy-MM-dd')
} else {
  try {
    # Validate format
    [void][datetime]::ParseExact($RunDate,'yyyy-MM-dd',$null)
  } catch {
    Write-WorkerTrace -Phase 'INPUT_VALIDATION_ERROR' -Level 'ERROR' -Details "Invalid RunDate=$RunDate"
    throw "Refresh-One.ps1: -RunDate must be yyyy-MM-dd (got '$RunDate')."
  }
}
Write-WorkerTrace -Phase 'INPUT_VALIDATION_END' -Details "RunDate=$RunDate"

# ---------- DB helper ----------
function Write-EventToDb {
  param(
    [string]$ConnStr,
    [string]$RunId,
    [string]$Batch,
    [ValidateSet('Refresh','Email')][string]$Stage,
    [DateTime]$TimestampUtc,
    [string]$RunDateStr,   # yyyy-MM-dd
    [string]$MasterPath,
    [string]$FilePath,
    [string]$Method,
    [ValidateSet('OK','FAIL','SKIP')][string]$Status,
    [string]$ErrorText,
    [int]$DurationS
  )

  $sql = @"
INSERT INTO events
(run_id,batch,stage,timestamp_utc,rundate,master_path,file_path,method,status,error_text,duration_s,recipients_to,subject)
VALUES
(@run,@batch,@stage,@ts,@rd,@mp,@fp,@m,@st,@err,@dur,NULL,NULL)
"@

  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    try {
      $conn.Open()
    } catch {
      $msg = $_.Exception.Message
      if ($msg -like "*RSA public key*not enabled*") {
        throw "MySQL connection refused: RSA public key retrieval not enabled. Add 'AllowPublicKeyRetrieval=True;SslMode=None' (or set up TLS). Raw: $msg"
      }
      throw "Could not open MySQL connection. $msg"
    }

    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $p = $cmd.Parameters

    # Use AddWithValue to avoid MySqlDbType enum references entirely
    [void]$p.AddWithValue("@run",   $RunId)
    [void]$p.AddWithValue("@batch", ($(if ($null -ne $Batch) { $Batch } else { "" })))
    [void]$p.AddWithValue("@stage", $Stage)
    [void]$p.AddWithValue("@ts",    $TimestampUtc)                                  # provider infers DATETIME
    [void]$p.AddWithValue("@rd",    [datetime]::ParseExact($RunDateStr,'yyyy-MM-dd',$null)) # provider infers DATE
    [void]$p.AddWithValue("@mp",    ($(if ($MasterPath) { $MasterPath } else { [DBNull]::Value })))
    [void]$p.AddWithValue("@fp",    $FilePath)
    [void]$p.AddWithValue("@m",     ($(if ($null -ne $Method) { $Method } else { "" }))) # VARCHAR
    [void]$p.AddWithValue("@st",    $Status)                                        # VARCHAR
    [void]$p.AddWithValue("@err",   ($(if ($ErrorText) { $ErrorText } else { [DBNull]::Value })))
    [void]$p.AddWithValue("@dur",   [int]$DurationS)                                # INT

    [void]$cmd.ExecuteNonQuery()
    return $true
  } catch {
    Write-Error "DB insert failed for '$FilePath': $($_.Exception.Message)"
    return $false
  } finally {
    if ($conn.State -ne 'Closed') { $conn.Close() }
    $conn.Dispose()
  }
}

# --- Per-run isolated temp folder to avoid cache collisions ---
Write-WorkerTrace -Phase 'TEMP_SETUP_START'
try {
  $tempRoot = "C:\Temp\ExcelTmp"
  New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
  $safeName = ([IO.Path]::GetFileNameWithoutExtension($Path) -replace '[^A-Za-z0-9_-]','_')
  $runTemp  = Join-Path $tempRoot ("{0}_{1}" -f $safeName, [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Force -Path $runTemp | Out-Null

  # Scope to *this process* only (won’t affect machine/user)
  [Environment]::SetEnvironmentVariable('TEMP', $runTemp, 'Process')
  [Environment]::SetEnvironmentVariable('TMP',  $runTemp, 'Process')
  Write-WorkerTrace -Phase 'TEMP_SETUP_END' -Details "Directory=$runTemp"
} catch {
  Write-WorkerTrace -Phase 'TEMP_SETUP_ERROR' -Level 'WARN' -Details "Continuing=True; Error=$($_.Exception.Message)"
}

# Handle the erros
function Clear-OfficeCaches {
  param([switch]$AlsoOfficeFileCache)
  try {
    $paths = @("$env:LOCALAPPDATA\Microsoft\Windows\INetCache\Content.MSO")
    if ($AlsoOfficeFileCache) {
      $paths += "$env:LOCALAPPDATA\Microsoft\Office\16.0\OfficeFileCache"
    }
    foreach ($p in $paths) {
      if (Test-Path $p) {
        Get-ChildItem -LiteralPath $p -Recurse -ErrorAction SilentlyContinue |
          Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
      }
    }
  } catch { }
}


# ---------- Refresh with targeted retries (cache + RPC) ----------
$status   = "OK"
$err      = ""
$t0       = Get-Date
$didCacheRetry = $false
$didRpcRetry   = $false
$attemptNumber = 0

:refresh_attempt do {
  $attemptNumber++
  Write-WorkerTrace -Phase 'ATTEMPT_START' -Details "Attempt=$attemptNumber"
  Register-ComMessageFilter
  Write-WorkerTrace -Phase 'COM_MESSAGE_FILTER_REGISTERED' -Details "Attempt=$attemptNumber"
  $excel = $null
  try {
    $excel = Start-Excel -MachineLimit $MachineExcelLimit -SlotWaitTimeoutSec $ExcelSlotWaitTimeoutSec -Trace $traceCallback
  } catch {
    Write-WorkerTrace -Phase 'ATTEMPT_START_ERROR' -Level 'ERROR' -Details "Attempt=$attemptNumber; Error=$($_.Exception.Message)"
    Unregister-ComMessageFilter
    Write-WorkerTrace -Phase 'COM_MESSAGE_FILTER_UNREGISTERED' -Details "Attempt=$attemptNumber"
    throw
  }
  try {
    # Wrap critical COM calls in Invoke-ComRetry so temporary busy states don’t blow up
    Invoke-ComRetry { Refresh-WorkbookSmart -excel $excel -Path $Path -TimeoutSec $TimeoutSec -FastMode:$FastMode -Trace $traceCallback } | Out-Null
    Write-WorkerTrace -Phase 'ATTEMPT_REFRESH_END' -Details "Attempt=$attemptNumber"
  }
  catch {
    $msg = $_.Exception.Message
    $hr  = $_.Exception.HResult
    Write-WorkerTrace -Phase 'ATTEMPT_ERROR' -Level 'ERROR' -Details "Attempt=$attemptNumber; HResult=$hr; Error=$msg"

    # 1) Temp/Office cache collision retry (your existing logic)
    if (-not $didCacheRetry -and ($msg -match 'INetCache\\Content\.MSO' -or $msg -match 'OfficeFileCache')) {
      Write-WorkerTrace -Phase 'CACHE_RETRY_START' -Level 'WARN' -Details "Attempt=$attemptNumber"
      try { Stop-Excel $excel -Trace $traceCallback } catch {}
      Write-WorkerTrace -Phase 'CACHE_CLEAR_START' -Level 'WARN'
      Clear-OfficeCaches
      Write-WorkerTrace -Phase 'CACHE_CLEAR_END' -Level 'WARN'
      Start-Sleep -Seconds 3
      $didCacheRetry = $true
      Write-WorkerTrace -Phase 'CACHE_RETRY_END' -Level 'WARN' -Details 'Retrying=True'
      continue refresh_attempt
    }

    # 2) RPC server unavailable => rebuild Excel once
    if (-not $didRpcRetry -and $hr -eq -2147023174) { # 0x800706BA
      Write-WorkerTrace -Phase 'RPC_RETRY_START' -Level 'WARN' -Details "Attempt=$attemptNumber; HResult=$hr"
      try { Stop-Excel $excel -Trace $traceCallback } catch {}
      Start-Sleep -Seconds 2
      $didRpcRetry = $true
      Write-WorkerTrace -Phase 'RPC_RETRY_END' -Level 'WARN' -Details 'Retrying=True'
      continue refresh_attempt
    }

    # Anything else — fail
    $status = "FAIL"
    $err    = $msg
  }
  finally {
    Write-WorkerTrace -Phase 'ATTEMPT_CLEANUP_START' -Details "Attempt=$attemptNumber"
    try { Stop-Excel $excel -Trace $traceCallback } catch {
      Write-WorkerTrace -Phase 'ATTEMPT_CLEANUP_ERROR' -Level 'WARN' -Details "Attempt=$attemptNumber; Error=$($_.Exception.Message)"
    }
    Unregister-ComMessageFilter
    Write-WorkerTrace -Phase 'COM_MESSAGE_FILTER_UNREGISTERED' -Details "Attempt=$attemptNumber"
    Write-WorkerTrace -Phase 'ATTEMPT_CLEANUP_END' -Details "Attempt=$attemptNumber"
  }
  break
} while ($true)


# ---------- Write event to DB ----------
$nowUtc  = (Get-Date).ToUniversalTime()
$duration = [int]((Get-Date) - $t0).TotalSeconds

try {
  Write-WorkerTrace -Phase 'DB_EVENT_WRITE_START' -Details "Status=$status; DurationS=$duration"
  $ok = Write-EventToDb `
    -ConnStr      $DbConn `
    -RunId        $LogIdentifier `
    -Batch        $Batch `
    -Stage        'Refresh' `
    -TimestampUtc $nowUtc `
    -RunDateStr   $RunDate `
    -MasterPath   $Master `
    -FilePath     $Path `
    -Method       $Method `
    -Status       $status `
    -ErrorText    $err `
    -DurationS    $duration

  $dbTraceLevel = if ($ok) { 'INFO' } else { 'ERROR' }
  Write-WorkerTrace -Phase 'DB_EVENT_WRITE_END' -Level $dbTraceLevel -Details "Success=$ok; Status=$status"

  if (-not $ok -and $status -eq 'OK') {
    # Refresh succeeded but DB write failed: surface a warning (non-fatal for Excel refresh)
    Write-Warning "Refresh succeeded, but DB logging failed for '$Path'. See errors above."
  }
} catch {
  # Defensive catch — shouldn't happen because Write-EventToDb catches its own
  Write-Error "Unexpected logging error: $($_.Exception.Message)"
  Write-WorkerTrace -Phase 'DB_EVENT_WRITE_ERROR' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
}

# Exit code for parent / scheduler
$finalExitCode = if ($status -eq 'OK') { 0 } else { 1 }
Write-WorkerTrace -Phase 'PROCESS_END' -Level $(if ($finalExitCode -eq 0) { 'INFO' } else { 'ERROR' }) -Details "Status=$status; DurationS=$duration; ExitCode=$finalExitCode"
exit $finalExitCode
