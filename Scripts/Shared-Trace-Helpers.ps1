<#
Concurrency-safe file tracing shared by the batch dispatcher and per-file workers.

The trace deliberately excludes command lines and database connection strings.
Every line is self-contained so the final line for a workbook identifies the last
phase reached before a crash, reboot, or hard Excel COM hang.
#>

$script:RefreshTraceMutexName = 'Global\ProjectReportingAutomation.RefreshTraceLog'

function ConvertTo-RefreshTraceField {
  param([AllowNull()][object]$Value)

  if ($null -eq $Value) { return '' }
  return (([string]$Value) -replace '[\r\n]+', ' ' -replace '\|', '/')
}

function Write-RefreshTrace {
  param(
    [Parameter(Mandatory=$true)][string]$TraceLogPath,
    [Parameter(Mandatory=$true)][string]$RunId,
    [string]$Batch = '',
    [string]$FilePath = '',
    [Parameter(Mandatory=$true)][string]$Phase,
    [ValidateSet('DEBUG','INFO','WARN','ERROR')][string]$Level = 'INFO',
    [string]$Source = 'Worker',
    [string]$Details = '',
    [int]$WorkerPid = $PID,
    [AllowNull()][object]$ExcelPid = $null,
    [AllowNull()][object]$ElapsedSeconds = $null
  )

  if ([string]::IsNullOrWhiteSpace($TraceLogPath)) { return }

  $elapsedText = if ($null -ne $ElapsedSeconds) {
    ([double]$ElapsedSeconds).ToString('0.000', [Globalization.CultureInfo]::InvariantCulture)
  } else {
    ''
  }

  $line = @(
    (Get-Date).ToString('o'),
    "Level=$(ConvertTo-RefreshTraceField $Level)",
    "Source=$(ConvertTo-RefreshTraceField $Source)",
    "RunId=$(ConvertTo-RefreshTraceField $RunId)",
    "Batch=$(ConvertTo-RefreshTraceField $Batch)",
    "WorkerPID=$WorkerPid",
    "ExcelPID=$(ConvertTo-RefreshTraceField $ExcelPid)",
    "Phase=$(ConvertTo-RefreshTraceField $Phase)",
    "ElapsedS=$elapsedText",
    "File=$(ConvertTo-RefreshTraceField $FilePath)",
    "Details=$(ConvertTo-RefreshTraceField $Details)"
  ) -join ' | '

  $traceMutex = $null
  $mutexAcquired = $false
  try {
    $traceDirectory = Split-Path -Parent $TraceLogPath
    if (-not [string]::IsNullOrWhiteSpace($traceDirectory)) {
      New-Item -ItemType Directory -Force -Path $traceDirectory -ErrorAction Stop | Out-Null
    }

    $createdNew = $false
    $traceMutex = [Threading.Mutex]::new($false, $script:RefreshTraceMutexName, [ref]$createdNew)
    try {
      $mutexAcquired = $traceMutex.WaitOne([TimeSpan]::FromSeconds(10))
    } catch [Threading.AbandonedMutexException] {
      # Ownership transfers to this process when the previous writer died.
      $mutexAcquired = $true
    }

    if (-not $mutexAcquired) {
      Write-Warning "Refresh trace write skipped after waiting 10 seconds for the log mutex: $TraceLogPath"
      return
    }

    $utf8NoBom = [Text.UTF8Encoding]::new($false)
    [IO.File]::AppendAllText($TraceLogPath, $line + [Environment]::NewLine, $utf8NoBom)
  } catch {
    # Diagnostics must never cause a report refresh to fail.
    Write-Warning "Refresh trace write failed for '$TraceLogPath': $($_.Exception.Message)"
  } finally {
    if ($mutexAcquired -and $null -ne $traceMutex) {
      try { $traceMutex.ReleaseMutex() } catch {}
    }
    if ($null -ne $traceMutex) {
      try { $traceMutex.Dispose() } catch {}
    }
  }
}
