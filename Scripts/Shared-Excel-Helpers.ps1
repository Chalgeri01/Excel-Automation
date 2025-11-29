<# ================== Shared-Excel-Helpers.ps1 ==================
Utilities:
- Start/Stop Excel (hidden)
- Wait for connections to finish
- Read master (B path, E new path, F method) → returns objects {Path, Method, Row}
- Refresh-WorkbookSmart (PQ then tables/pivots-if-present, FastMode optional)
#>

# --- COM Message Filter to handle RPC_E_CALL_REJECTED from Excel ---
if (-not ([System.Management.Automation.PSTypeName]'ComMessageFilter').Type) {
  Add-Type -Language CSharp -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public class ComMessageFilter : IOleMessageFilter
{
    // Register the message filter.
    public static void Register() {
        IOleMessageFilter newFilter = new ComMessageFilter();
        IOleMessageFilter oldFilter = null;
        CoRegisterMessageFilter(newFilter, out oldFilter);
    }

    // Revoke the message filter.
    public static void Revoke() {
        IOleMessageFilter oldFilter = null;
        CoRegisterMessageFilter(null, out oldFilter);
    }

    // Handle incoming call.
    int IOleMessageFilter.HandleInComingCall(int dwCallType, System.IntPtr hTaskCaller, int dwTickCount, System.IntPtr lpInterfaceInfo) {
        // SERVERCALL_ISHANDLED
        return 0;
    }

    // Thread call rejected/retry—tell COM to retry after a short delay.
    int IOleMessageFilter.RetryRejectedCall(System.IntPtr hTaskCallee, int dwTickCount, int dwRejectType) {
        // SERVERCALL_RETRYLATER = 2 -> ask COM to retry after 100 ms
        if (dwRejectType == 2) return 100;
        // cancel call
        return -1;
    }

    int IOleMessageFilter.MessagePending(System.IntPtr hTaskCallee, int dwTickCount, int dwPendingType) {
        // PENDINGMSG_WAITDEFPROCESS
        return 2;
    }

    [DllImport("Ole32.dll")]
    private static extern int CoRegisterMessageFilter(IOleMessageFilter newFilter, out IOleMessageFilter oldFilter);
}

[ComImport(), Guid("00000016-0000-0000-C000-000000000046"),
 InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
public interface IOleMessageFilter {
    [PreserveSig] int HandleInComingCall(int dwCallType, IntPtr hTaskCaller, int dwTickCount, IntPtr lpInterfaceInfo);
    [PreserveSig] int RetryRejectedCall(IntPtr hTaskCallee, int dwTickCount, int dwRejectType);
    [PreserveSig] int MessagePending(IntPtr hTaskCallee, int dwTickCount, int dwPendingType);
}
"@
}

function Register-ComMessageFilter { 
  try { [ComMessageFilter]::Register() } catch { }
}
function Unregister-ComMessageFilter { 
  try { [ComMessageFilter]::Revoke() } catch { }
}

function Invoke-ComRetry {
  param(
    [Parameter(Mandatory=$true)][scriptblock]$ScriptBlock,
    [int]$MaxAttempts = 6,           # ~6 tries
    [int]$InitialDelayMs = 150       # start with 150ms, we’ll backoff
  )
  $delay = $InitialDelayMs
  for ($i=1; $i -le $MaxAttempts; $i++) {
    try {
      return & $ScriptBlock
    } catch {
      $h = ($_.Exception.HResult)
      # 0x80010001 = RPC_E_CALL_REJECTED (Excel busy), 0x800706BA = RPC server unavailable
      if (($h -eq -2147418111) -or ($h -eq -2147023174)) {
        Start-Sleep -Milliseconds $delay
        $delay = [Math]::Min($delay * 2, 2000) # cap at 2s
        continue
      } else {
        throw
      }
    }
  }
  throw "Invoke-ComRetry: exceeded retries for COM call."
}


function Start-Excel {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false
  try { $excel.ScreenUpdating   = $false } catch {}
  try { $excel.DisplayStatusBar = $false } catch {}
  try { $excel.EnableEvents     = $false } catch {}
  try { $excel.Calculation      = -4135 }  catch {} # xlCalculationManual
  return $excel
}

function Stop-Excel($excel){
  if ($null -ne $excel){
    try { $excel.Quit() } catch {}
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    [gc]::Collect(); [gc]::WaitForPendingFinalizers()
  }
}

function Wait-Connections($wb, [int]$TimeoutSec=1400){
  $sw = [Diagnostics.Stopwatch]::StartNew()
  do {
    Start-Sleep -Milliseconds 200
    $any = $false
    foreach($cn in @($wb.Connections)){
      try { if ($cn.Refreshing){ $any = $true; break } } catch {}
    }
    if (-not $any){ $sw.Stop(); return $true }
  } while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec)
  $sw.Stop(); return $false
}

function Get-PathsFromMaster([string]$MasterPath,[string]$SheetName,[string]$PathColumn='B',[int]$StartRow=2,[int]$EndRow=0){
  if (-not (Test-Path $MasterPath)){ throw "Master file not found: $MasterPath" }
  $excel = Start-Excel
  $wb = $null
  try {
    $wb = $excel.Workbooks.Open($MasterPath, $false, $true)  # ReadOnly
    $ws = if ([string]::IsNullOrWhiteSpace($SheetName)) { $wb.Worksheets.Item(1) } else { $wb.Worksheets.Item($SheetName) }
    $last = if ($EndRow -gt 0) { $EndRow } else { $ws.Cells($ws.Rows.Count, $PathColumn).End(-4162).Row } # xlUp

    $items = New-Object System.Collections.Generic.List[object]
    for($r=$StartRow; $r -le $last; $r++){
      $b = [string]$ws.Range("B$r").Value()       # Report Path
      if ([string]::IsNullOrWhiteSpace($b)) { continue }
      $e = [string]$ws.Range("E$r").Value()       # New Path (override)
      $f = [string]$ws.Range("F$r").Value()       # Method (Email/Local)
      $final = if (-not [string]::IsNullOrWhiteSpace($e)) { $e.Trim() } else { $b.Trim() }
      $items.Add([pscustomobject]@{
        Path   = $final
        Method = if ($f) { $f.Trim() } else { "" }
        Row    = $r
      })
    }
    return ,$items.ToArray()
  }
  finally {
    if ($wb -ne $null){ try { $wb.Close($false) } catch {} }
    Stop-Excel $excel
  }
}

function Old_Refresh-WorkbookSmart([object]$excel,[string]$Path,[int]$TimeoutSec=900,[switch]$FastMode){
  if (-not (Test-Path $Path)){ throw "File not found: $Path" }

  $wb = $null
  try {
    $wb = $excel.Workbooks.Open($Path, $false, $false) # read/write

    if (-not $FastMode) {
      foreach($cn in @($wb.Connections)){
        try {
          if ($cn.Type -eq 1 -and $cn.ODBCConnection) { $cn.ODBCConnection.BackgroundQuery = $false }  # ODBC
          if ($cn.Type -eq 2 -and $cn.OLEDBConnection){ $cn.OLEDBConnection.BackgroundQuery = $false } # OLEDB
        } catch {}
      }
    }

    # Power Query / Connections
    $wb.RefreshAll() | Out-Null
    try { $excel.CalculateUntilAsyncQueriesDone() } catch {}
    [void](Wait-Connections -wb $wb -TimeoutSec $TimeoutSec)

    # Only refresh tables/pivots if present (and not in FastMode)
    if (-not $FastMode){
      $hasTables = $false; $hasPivots = $false
      foreach($ws in @($wb.Worksheets)){
        try { if ($ws.ListObjects().Count -gt 0){ $hasTables = $true } } catch {}
        try { if ($ws.PivotTables().Count -gt 0){ $hasPivots = $true } } catch {}
      }
      if ($hasTables){
        foreach($ws in @($wb.Worksheets)){
          foreach($lo in @($ws.ListObjects)){
            try { if ($lo.QueryTable -ne $null){ $lo.Refresh() | Out-Null } } catch {}
          }
        }
      }
      if ($hasPivots){
        foreach($ws in @($wb.Worksheets)){
          foreach($pt in @($ws.PivotTables)){
            try { $pt.RefreshTable() | Out-Null } catch {}
          }
        }
      }
    }
    try { if ($wb.Model) { $wb.Model.Refresh(); Start-Sleep -Seconds 5 } } catch {}
    try { $excel.CalculateFull() } catch {}
    # try { $excel.CalculateFullRebuild() } catch {}  # only if needed
    $wb.Save()
  }
  finally {
    if ($wb -ne $null){ try { $wb.Close($true) } catch {} }
  }
}

function Refresh-WorkbookSmart([object]$excel,[string]$Path,[int]$TimeoutSec=900,[switch]$FastMode){
  if (-not (Test-Path $Path)){ throw "File not found: $Path" }

  $wb = $null
  try {
    # Open (COM-retry is safer, but plain open is fine if you prefer)
    $wb = $excel.Workbooks.Open($Path, $false, $false) # read/write

    # --- Disable background queries, null-safe
    if (-not $FastMode) {
      $cnCol = $null; try { $cnCol = $wb.Connections } catch {}
      foreach ($cn in (Get-ItemsSafe $cnCol)) {
        try {
          if ($cn.Type -eq 1 -and $cn.ODBCConnection)  { $cn.ODBCConnection.BackgroundQuery  = $false }
          if ($cn.Type -eq 2 -and $cn.OLEDBConnection) { $cn.OLEDBConnection.BackgroundQuery = $false }
        } catch {}
      }
    }

    # --- Power Query / Connections (guard everything that can flap to $null)
    try { Invoke-ComRetry { $wb.RefreshAll() | Out-Null } } catch {}
    try { $excel.CalculateUntilAsyncQueriesDone() } catch {}

    $okWait = $false
    try { $okWait = (Wait-Connections -wb $wb -TimeoutSec $TimeoutSec) } catch { $okWait = $true }

    # --- Only refresh tables/pivots if present (and not in FastMode)
    if (-not $FastMode){
      $hasTables = $false; $hasPivots = $false
      $wsList = Get-ItemsSafe $wb.Worksheets

      foreach ($ws in $wsList) {
        $loCol = $null; try { $loCol = $ws.ListObjects } catch {}
        if (Get-CountSafe $loCol -gt 0) { $hasTables = $true }

        $ptCol = $null; try { $ptCol = $ws.PivotTables } catch {}
        if (Get-CountSafe $ptCol -gt 0) { $hasPivots = $true }
      }

      if ($hasTables){
        foreach ($ws in $wsList) {
          $loCol = $null; try { $loCol = $ws.ListObjects } catch {}
          foreach ($lo in (Get-ItemsSafe $loCol)) {
            try { if ($lo) { Invoke-ComRetry { $lo.Refresh() | Out-Null } } } catch {}
          }
        }
      }

      if ($hasPivots){
        foreach ($ws in $wsList) {
          $ptCol = $null; try { $ptCol = $ws.PivotTables } catch {}
          foreach ($pt in (Get-ItemsSafe $ptCol)) {
            try { if ($pt) { Invoke-ComRetry { $pt.RefreshTable() | Out-Null } } } catch {}
          }
        }
      }
    }

    # --- Data model + calc (safe)
    try { if ($wb.Model) { $wb.Model.Refresh(); Start-Sleep -Seconds 5 } } catch {}
    try { $excel.CalculateFull() } catch {}
    # try { $excel.CalculateFullRebuild() } catch {}  # only if really needed

    $wb.Save()
  }
  finally {
    if ($wb -ne $null){ try { $wb.Close($true) } catch {} }
  }
}
