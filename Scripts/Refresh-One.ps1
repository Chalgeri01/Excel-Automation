# Refresh-One.ps1 — DB-backed single workbook refresh + event write
param(
  [Parameter(Mandatory = $true)][string]$Path,
  [int]$TimeoutSec = 900,
  [switch]$FastMode,
  [string]$Master = "",
  [string]$Method = "",
  [string]$Batch  = "",
  # --- DB logging (required in DB mode) ---
  [Parameter(Mandatory = $true)][string]$DbConn,
  [Parameter(Mandatory = $true)][string]$LogIdentifier,   # e.g. run-log_YYYY-MM-DD_Batch-1
  [string]$RunDate = ""                                    # yyyy-MM-dd (logical day for this run)
)

. "$PSScriptRoot\Shared-Excel-Helpers.ps1"

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
Load-MySqlAssembly

# ---------- Param validation / normalization ----------
if (-not (Test-Path -LiteralPath $Path)) {
  throw "Refresh-One.ps1: file not found: $Path"
}
if ([string]::IsNullOrWhiteSpace($DbConn)) {
  throw "Refresh-One.ps1: Missing -DbConn (MySQL connection string)."
}
if ([string]::IsNullOrWhiteSpace($LogIdentifier)) {
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
    throw "Refresh-One.ps1: -RunDate must be yyyy-MM-dd (got '$RunDate')."
  }
}

# ---------- DB helper ----------
function Write-EventToDb_old {
  param(
    [string]$ConnStr,
    [string]$RunId,
    [string]$Batch,
    [ValidateSet('Refresh','Email')][string]$Stage,  # here we use 'Refresh'
    [DateTime]$TimestampUtc,
    [string]$RunDateStr,                             # yyyy-MM-dd
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

  # Try open connection with good diagnostics
  $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($ConnStr)
  try {
    try {
      $conn.Open()
    } catch {
      $msg = $_.Exception.Message
      if ($msg -like "*RSA public key*not enabled*") {
        throw "MySQL connection refused: RSA public key retrieval not enabled. Add 'AllowPublicKeyRetrieval=True;SslMode=None' to your connection string (or enable proper TLS). Understood error: $msg"
      }
      throw "Could not open MySQL connection. Check host/port/user/password. Error: $msg"
    }

    $cmd = $conn.CreateCommand()
    $cmd.CommandText = $sql
    $p = $cmd.Parameters

    # Correct enum casing matters (e.g. DateTime, VarChar, Int32, LongText)
    $null = $p.Add("@run",   [MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = $RunId
    $null = $p.Add("@batch", [MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = ($Batch ?? "")
    $null = $p.Add("@stage", [MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = $Stage
    $null = $p.Add("@ts",    [MySql.Data.MySqlClient.MySqlDbType]::DateTime).Value  = $TimestampUtc
    $null = $p.Add("@rd",    [MySql.Data.MySqlClient.MySqlDbType]::Date).Value      = [datetime]::ParseExact($RunDateStr,'yyyy-MM-dd',$null)
    $null = $p.Add("@mp",    [MySql.Data.MySqlClient.MySqlDbType]::LongText).Value  = ($MasterPath ?? [DBNull]::Value)
    $null = $p.Add("@fp",    [MySql.Data.MySqlClient.MySqlDbType]::LongText).Value  = $FilePath
    $null = $p.Add("@m",     [MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = ($Method ?? "")
    $null = $p.Add("@st",    [MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value   = $Status
    $null = $p.Add("@err",   [MySql.Data.MySqlClient.MySqlDbType]::LongText).Value  = ($(if ($ErrorText) { $ErrorText } else { [DBNull]::Value }))
    $null = $p.Add("@dur",   [MySql.Data.MySqlClient.MySqlDbType]::Int32).Value     = [int]$DurationS

    $null = $cmd.ExecuteNonQuery()
    return $true
  } catch {
    Write-Error "DB insert failed for '$FilePath': $($_.Exception.Message)"
    return $false
  } finally {
    if ($conn.State -ne 'Closed') { $conn.Close() }
    $conn.Dispose()
  }
}

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
    [void]$p.AddWithValue("@batch", ($Batch   ?? ""))
    [void]$p.AddWithValue("@stage", $Stage)
    [void]$p.AddWithValue("@ts",    $TimestampUtc)                                  # provider infers DATETIME
    [void]$p.AddWithValue("@rd",    [datetime]::ParseExact($RunDateStr,'yyyy-MM-dd',$null)) # provider infers DATE
    [void]$p.AddWithValue("@mp",    ($(if ($MasterPath) { $MasterPath } else { [DBNull]::Value })))
    [void]$p.AddWithValue("@fp",    $FilePath)
    [void]$p.AddWithValue("@m",     ($Method ?? ""))                                # VARCHAR
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
try {
  $tempRoot = "C:\Temp\ExcelTmp"
  New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
  $safeName = ([IO.Path]::GetFileNameWithoutExtension($Path) -replace '[^A-Za-z0-9_-]','_')
  $runTemp  = Join-Path $tempRoot ("{0}_{1}" -f $safeName, [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Force -Path $runTemp | Out-Null

  # Scope to *this process* only (won’t affect machine/user)
  [Environment]::SetEnvironmentVariable('TEMP', $runTemp, 'Process')
  [Environment]::SetEnvironmentVariable('TMP',  $runTemp, 'Process')
} catch { }

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


# ---------- Refresh with one targeted retry ----------
$status   = "OK"
$err      = ""
$t0       = Get-Date
$didRetry = $false

:refresh_attempt do {
  $excel = Start-Excel
  try {
    Refresh-WorkbookSmart -excel $excel -Path $Path -TimeoutSec $TimeoutSec -FastMode:$FastMode
  }
  catch {
    $msg = $_.Exception.Message
    # Trigger retry only for the known cache/collision signatures
    if (-not $didRetry -and ($msg -match 'INetCache\\Content\.MSO' -or $msg -match 'OfficeFileCache')) {
      try { Stop-Excel $excel } catch {}
      Clear-OfficeCaches            # clear cache that caused the lock
      Start-Sleep -Seconds 3
      $didRetry = $true
      continue refresh_attempt      # restart Excel and try once more
    } else {
      $status = "FAIL"
      $err    = $msg
    }
  }
  finally {
    try { Stop-Excel $excel } catch {}
  }
  break
} while ($true)

# ---------- Write event to DB ----------
$nowUtc  = (Get-Date).ToUniversalTime()
$duration = [int]((Get-Date) - $t0).TotalSeconds

try {
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

  if (-not $ok -and $status -eq 'OK') {
    # Refresh succeeded but DB write failed: surface a warning (non-fatal for Excel refresh)
    Write-Warning "Refresh succeeded, but DB logging failed for '$Path'. See errors above."
  }
} catch {
  # Defensive catch — shouldn't happen because Write-EventToDb catches its own
  Write-Error "Unexpected logging error: $($_.Exception.Message)"
}

# Exit code for parent / scheduler
if ($status -eq 'OK') { exit 0 } else { exit 1 }
