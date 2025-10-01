<# Launch-RunParallel.ps1 — robust launcher for Task Scheduler
    - Logs to C:\ProgramData\ReportRunner\logs
    - Optional SMB pre-auth (net use) for UNC shares
    - Uses local cache copy of Master workbook to avoid Excel UNC lock issues
#>

#region ==== CONFIG ====
# PowerShell script to run
$ScriptPath = 'C:\Users\kapl\Desktop\Project-Reporting-Automation\Scripts\Run-Parallel.ps1'

# Working directory (Task Scheduler "Start in")
$StartIn    = 'C:\Users\kapl\Desktop\Project-Reporting-Automation\Scripts'

# Parameters for Run-Parallel.ps1
$Params = @{
  MasterPath    = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\01_Data_Update-11.00PMV2.xlsx"
  SheetName     = ''
  PathColumn    = 'B'
  StartRow      = 2
  LogPath       = 'C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\run-log.csv'
  ThrottleLimit = 3
  Batch         = '23:00'
}

# Logging folder
$LogDir = 'C:\ReportRunner\logs'

# ---- OPTIONAL: SMB pre-auth for UNC path (use only if needed) ----
$UseNetUse  = $false
$ShareRoot  = '\\192.168.1.237\Accounts'
$ShareUser  = 'KAPL\kapl'
$SharePass  = 'Smart@it#2025'
#endregion ==== CONFIG ====

# ---- Prep logging ----
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Transcript = Join-Path $LogDir 'launch-runparallel-transcript.txt'
$TraceFile  = Join-Path $LogDir 'launcher-trace.txt'
"=== $(Get-Date -Format s) : starting ===" | Out-File $TraceFile -Append -Encoding utf8

# Helper: safe message to trace
function Write-Trace([string]$msg) {
  ("[{0}] {1}" -f (Get-Date -Format s), $msg) | Out-File $TraceFile -Append -Encoding utf8
}

# ---- Begin run ----
$exitCode = 0
$connectedShare = $false
try {
  Start-Transcript -Path $Transcript -Append
  Write-Trace "Transcript started. User=$env:USERNAME  PWD=$PWD"

  if (Test-Path $StartIn) {
    Set-Location $StartIn
    Write-Trace "Set-Location to $StartIn"
  }

  if (-not (Test-Path $ScriptPath)) {
    throw "Run-Parallel.ps1 not found: $ScriptPath"
  }

  if ($UseNetUse) {
    Write-Trace "Attempting SMB connect to $ShareRoot as $ShareUser"
    cmd /c "net use $ShareRoot /delete /y" | Out-Null
    $null = cmd /c "net use $ShareRoot /user:$ShareUser $SharePass /persistent:no"
    if ($LASTEXITCODE -ne 0) { throw "SMB connect failed" }
    $connectedShare = $true
    Write-Trace "SMB connect OK."
  } else {
    Write-Trace "SMB pre-auth disabled (UseNetUse=$UseNetUse). Assuming Credential Manager handles UNC."
  }

  # === NEW: Local cache for master workbook ===
  $originalMaster = $Params['MasterPath']
  if (-not (Test-Path $originalMaster)) {
    throw "Master workbook not reachable: $originalMaster"
  }
  Write-Trace "Master workbook reachable."

  $CacheDir = "C:\ReportRunner\cache"
  New-Item -ItemType Directory -Force -Path $CacheDir | Out-Null

  # Build local cache filename (hashed prefix to avoid collisions)
  $masterId = ([BitConverter]::ToString(
    (New-Object Security.Cryptography.MD5CryptoServiceProvider).ComputeHash(
      [Text.Encoding]::UTF8.GetBytes($originalMaster)
    )
  )).Replace('-', '').Substring(0,10)

  $safeName = [IO.Path]::GetFileName($originalMaster)
  $localMaster = Join-Path $CacheDir ("{0}-{1}" -f $masterId, $safeName)

  Copy-Item -Path $originalMaster -Destination $localMaster -Force
  Write-Trace "Copied master to local cache: $localMaster"

  # Override param to use local copy
  $Params['MasterPath'] = $localMaster
  $Script:LocalMasterToDelete = $localMaster
  # === END NEW ===

  Write-Trace "Invoking: $ScriptPath with params $($Params.Keys -join ', ')"
  & $ScriptPath @Params -ErrorAction Stop
  $exitCode = 0
  Write-Trace "Run-Parallel.ps1 completed successfully."
}
catch {
  $msg = $_.Exception.Message
  Write-Host "ERROR: $msg"
  Write-Trace "ERROR: $msg"
  $exitCode = 1
}
finally {
  if ($connectedShare) {
    Write-Trace "Disconnecting SMB: $ShareRoot"
    cmd /c "net use $ShareRoot /delete /y" | Out-Null
  }

  # === NEW: Cleanup cache ===
  if ($Script:LocalMasterToDelete -and (Test-Path $Script:LocalMasterToDelete)) {
    Write-Trace "Deleting cached master: $Script:LocalMasterToDelete"
    Remove-Item -Force -ErrorAction SilentlyContinue $Script:LocalMasterToDelete
  }
  # Purge any cache files older than 3 days
  Get-ChildItem -Path $CacheDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-3) } |
    Remove-Item -Force -ErrorAction SilentlyContinue
  # === END NEW ===

  Write-Trace "Exiting with code $exitCode"
  Stop-Transcript | Out-Null
  exit $exitCode
}
