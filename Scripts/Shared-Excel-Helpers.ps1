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

if (-not ([System.Management.Automation.PSTypeName]'ExcelProcessNative').Type) {
  Add-Type -Language CSharp -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

public static class ExcelProcessNative
{
    [DllImport("user32.dll")]
    public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
}
"@
}

if ($null -eq $script:OwnedExcelProcessIds) {
  $script:OwnedExcelProcessIds = @{}
}

if ($null -eq $script:OwnedExcelSlots) {
  $script:OwnedExcelSlots = @{}
}

$script:ExcelConcurrencyMutexPrefix = 'Global\ProjectReportingAutomation.ExcelRefresh.Slot'

function Enter-ExcelConcurrencySlot {
  param(
    [ValidateRange(1,16)][int]$MachineLimit = 3,
    [ValidateRange(1,86400)][int]$WaitTimeoutSec = 21600
  )

  $mutexes = New-Object 'System.Collections.Generic.List[System.Threading.Mutex]'
  try {
    for ($slotNumber = 1; $slotNumber -le $MachineLimit; $slotNumber++) {
      $createdNew = $false
      $mutexName = '{0}{1}' -f $script:ExcelConcurrencyMutexPrefix, $slotNumber
      $mutex = [System.Threading.Mutex]::new($false, $mutexName, [ref]$createdNew)
      [void]$mutexes.Add($mutex)
    }

    [System.Threading.WaitHandle[]]$handles = $mutexes.ToArray()
    $timeoutMilliseconds = [int]([Math]::Min($WaitTimeoutSec * 1000L, [int]::MaxValue))

    try {
      $slotIndex = [System.Threading.WaitHandle]::WaitAny($handles, $timeoutMilliseconds)
    } catch [System.Threading.AbandonedMutexException] {
      # The previous worker died while holding this slot. WaitAny grants the
      # abandoned mutex to this worker, so it is safe to continue using it.
      $slotIndex = $_.Exception.MutexIndex
    }

    if ($slotIndex -eq [System.Threading.WaitHandle]::WaitTimeout) {
      throw "Timed out after $WaitTimeoutSec seconds waiting for one of $MachineLimit machine-wide Excel slots."
    }
    if ($slotIndex -lt 0 -or $slotIndex -ge $mutexes.Count) {
      throw "Could not determine the acquired machine-wide Excel slot."
    }

    $acquiredMutex = $mutexes[$slotIndex]
    for ($i = 0; $i -lt $mutexes.Count; $i++) {
      if ($i -ne $slotIndex) {
        try { $mutexes[$i].Dispose() } catch {}
      }
    }

    return [pscustomobject]@{
      Mutex      = $acquiredMutex
      SlotNumber = $slotIndex + 1
    }
  } catch {
    foreach ($mutex in $mutexes) {
      try { $mutex.Dispose() } catch {}
    }
    throw
  }
}

function Exit-ExcelConcurrencySlot($Slot) {
  if ($null -eq $Slot -or $null -eq $Slot.Mutex) { return }

  try {
    $Slot.Mutex.ReleaseMutex()
  } catch {
    Write-Warning "Could not release Excel concurrency slot $($Slot.SlotNumber): $($_.Exception.Message)"
  } finally {
    try { $Slot.Mutex.Dispose() } catch {}
  }
}

function Get-ExcelProcessId($excel) {
  if ($null -eq $excel) { return $null }

  try {
    $hwnd = [IntPtr]([int64]$excel.Hwnd)
    if ($hwnd -eq [IntPtr]::Zero) { return $null }

    [uint32]$excelProcessId = 0
    [void][ExcelProcessNative]::GetWindowThreadProcessId($hwnd, [ref]$excelProcessId)
    if ($excelProcessId -gt 0) { return [int]$excelProcessId }
  } catch {}

  return $null
}

function Invoke-ExcelTrace {
  param(
    [AllowNull()][scriptblock]$Trace,
    [Parameter(Mandatory=$true)][string]$Phase,
    [string]$Details = '',
    [ValidateSet('DEBUG','INFO','WARN','ERROR')][string]$Level = 'INFO',
    [AllowNull()][object]$ExcelPid = $null,
    [string]$Source = 'Excel'
  )

  if ($null -eq $Trace) { return }
  try {
    $null = & $Trace $Phase $Details $Level $ExcelPid $Source
  } catch {
    # Diagnostics must never change the Excel refresh outcome.
  }
}

function Start-Excel {
  param(
    [ValidateRange(1,16)][int]$MachineLimit = 3,
    [ValidateRange(1,86400)][int]$SlotWaitTimeoutSec = 21600,
    [AllowNull()][scriptblock]$Trace = $null
  )

  Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_SLOT_WAIT_START' -Details "MachineLimit=$MachineLimit; WaitTimeoutSec=$SlotWaitTimeoutSec"
  try {
    $excelSlot = Enter-ExcelConcurrencySlot -MachineLimit $MachineLimit -WaitTimeoutSec $SlotWaitTimeoutSec
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_SLOT_ACQUIRED' -Details "Slot=$($excelSlot.SlotNumber); MachineLimit=$MachineLimit"
  } catch {
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_SLOT_WAIT_ERROR' -Level 'ERROR' -Details "Error=$($_.Exception.Message)"
    throw
  }

  $excel = $null
  $instanceKey = $null

  try {
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_CREATE_START' -Details "Slot=$($excelSlot.SlotNumber)"
    $excel = New-Object -ComObject Excel.Application
    $instanceKey = [System.Runtime.CompilerServices.RuntimeHelpers]::GetHashCode($excel)
    $excelProcessId = Get-ExcelProcessId $excel
    if ($excelProcessId) {
      $script:OwnedExcelProcessIds[$instanceKey] = $excelProcessId
    }
    $script:OwnedExcelSlots[$instanceKey] = $excelSlot

    $excel.Visible = $false
    $excel.DisplayAlerts = $false
    try { $excel.ScreenUpdating   = $false } catch {}
    try { $excel.DisplayStatusBar = $false } catch {}
    try { $excel.EnableEvents     = $false } catch {}
    try { $excel.Calculation      = -4135 }  catch {} # xlCalculationManual
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_CREATE_END' -Details "Slot=$($excelSlot.SlotNumber); Calculation=Manual" -ExcelPid $excelProcessId
    return $excel
  } catch {
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_CREATE_ERROR' -Level 'ERROR' -Details "Slot=$($excelSlot.SlotNumber); Error=$($_.Exception.Message)" -ExcelPid $excelProcessId
    if ($null -ne $excel) {
      try { $excel.Quit() } catch {}
      try { [System.Runtime.InteropServices.Marshal]::FinalReleaseComObject($excel) | Out-Null } catch {}
    }
    if ($null -ne $instanceKey) {
      [void]$script:OwnedExcelProcessIds.Remove($instanceKey)
      [void]$script:OwnedExcelSlots.Remove($instanceKey)
    }
    Exit-ExcelConcurrencySlot $excelSlot
    throw
  }
}

function Stop-Excel {
  param(
    [AllowNull()][object]$excel,
    [AllowNull()][scriptblock]$Trace = $null
  )

  if ($null -ne $excel){
    $instanceKey = [System.Runtime.CompilerServices.RuntimeHelpers]::GetHashCode($excel)
    $excelProcessId = $script:OwnedExcelProcessIds[$instanceKey]
    $excelSlot = $script:OwnedExcelSlots[$instanceKey]
    if (-not $excelProcessId) {
      $excelProcessId = Get-ExcelProcessId $excel
    }

    $ownedProcess = $null
    if ($excelProcessId) {
      try { $ownedProcess = Get-Process -Id $excelProcessId -ErrorAction Stop } catch {}
    }

    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_STOP_START' -Details "Slot=$($excelSlot.SlotNumber)" -ExcelPid $excelProcessId
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_QUIT_START' -ExcelPid $excelProcessId
    try {
      $excel.Quit()
      Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_QUIT_END' -ExcelPid $excelProcessId
    } catch {
      Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_QUIT_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -ExcelPid $excelProcessId
    }

    Invoke-ExcelTrace -Trace $Trace -Phase 'COM_RELEASE_START' -ExcelPid $excelProcessId
    try {
      [System.Runtime.InteropServices.Marshal]::FinalReleaseComObject($excel) | Out-Null
      Invoke-ExcelTrace -Trace $Trace -Phase 'COM_RELEASE_END' -ExcelPid $excelProcessId
    } catch {
      Invoke-ExcelTrace -Trace $Trace -Phase 'COM_RELEASE_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -ExcelPid $excelProcessId
    }
    [gc]::Collect(); [gc]::WaitForPendingFinalizers()

    if ($ownedProcess) {
      $exited = $false
      Invoke-ExcelTrace -Trace $Trace -Phase 'PROCESS_EXIT_WAIT_START' -Details 'WaitMilliseconds=5000' -ExcelPid $excelProcessId
      try { $exited = $ownedProcess.WaitForExit(5000) } catch { $exited = $true }
      if (-not $exited) {
        Invoke-ExcelTrace -Trace $Trace -Phase 'PROCESS_FORCE_KILL_START' -Level 'WARN' -ExcelPid $excelProcessId
        try {
          Stop-Process -Id $excelProcessId -Force -ErrorAction Stop
          Invoke-ExcelTrace -Trace $Trace -Phase 'PROCESS_FORCE_KILL_END' -Level 'WARN' -ExcelPid $excelProcessId
        } catch {
          Write-Warning "Could not terminate owned Excel PID $excelProcessId : $($_.Exception.Message)"
          Invoke-ExcelTrace -Trace $Trace -Phase 'PROCESS_FORCE_KILL_ERROR' -Level 'ERROR' -Details "Error=$($_.Exception.Message)" -ExcelPid $excelProcessId
        }
      } else {
        Invoke-ExcelTrace -Trace $Trace -Phase 'PROCESS_EXITED' -ExcelPid $excelProcessId
      }
    }

    [void]$script:OwnedExcelProcessIds.Remove($instanceKey)
    [void]$script:OwnedExcelSlots.Remove($instanceKey)
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_SLOT_RELEASE_START' -Details "Slot=$($excelSlot.SlotNumber)" -ExcelPid $excelProcessId
    Exit-ExcelConcurrencySlot $excelSlot
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_SLOT_RELEASE_END' -Details "Slot=$($excelSlot.SlotNumber)" -ExcelPid $excelProcessId
    Invoke-ExcelTrace -Trace $Trace -Phase 'EXCEL_STOP_END' -ExcelPid $excelProcessId
  }
}

function Wait-Connections {
  param(
    $wb,
    [int]$TimeoutSec = 1400,
    [AllowNull()][scriptblock]$Trace = $null,
    [ValidateRange(10,3600)][int]$ProgressIntervalSec = 60
  )

  $sw = [Diagnostics.Stopwatch]::StartNew()
  $nextProgressAt = $ProgressIntervalSec
  do {
    Start-Sleep -Milliseconds 200
    $any = $false
    foreach($cn in @($wb.Connections)){
      try { if ($cn.Refreshing){ $any = $true; break } } catch {}
    }
    if (-not $any){ $sw.Stop(); return $true }
    if ($sw.Elapsed.TotalSeconds -ge $nextProgressAt) {
      Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_WAIT_PROGRESS' -Details ("ElapsedS={0:0}; TimeoutSec={1}" -f $sw.Elapsed.TotalSeconds,$TimeoutSec) -Source 'Workbook'
      $nextProgressAt += $ProgressIntervalSec
    }
  } while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec)
  $sw.Stop(); return $false
}

function Get-PathsFromMaster(
  [string]$MasterPath,
  [string]$SheetName,
  [string]$PathColumn='B',
  [int]$StartRow=2,
  [int]$EndRow=0,
  [ValidateRange(1,16)][int]$MachineExcelLimit=3,
  [ValidateRange(1,86400)][int]$ExcelSlotWaitTimeoutSec=21600
){
  if (-not (Test-Path $MasterPath)){ throw "Master file not found: $MasterPath" }
  $excel = Start-Excel -MachineLimit $MachineExcelLimit -SlotWaitTimeoutSec $ExcelSlotWaitTimeoutSec
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

function Refresh-WorkbookSmart {
  param(
    [object]$excel,
    [string]$Path,
    [int]$TimeoutSec = 900,
    [switch]$FastMode,
    [AllowNull()][scriptblock]$Trace = $null
  )

  Invoke-ExcelTrace -Trace $Trace -Phase 'FILE_PATH_CHECK_START' -Details "TimeoutSec=$TimeoutSec; FastMode=$($FastMode.IsPresent)" -Source 'Workbook'
  if (-not (Test-Path $Path)){
    Invoke-ExcelTrace -Trace $Trace -Phase 'FILE_PATH_CHECK_ERROR' -Level 'ERROR' -Details 'File not found' -Source 'Workbook'
    throw "File not found: $Path"
  }
  Invoke-ExcelTrace -Trace $Trace -Phase 'FILE_PATH_CHECK_END' -Source 'Workbook'

  $wb = $null
  try {
    Invoke-ExcelTrace -Trace $Trace -Phase 'WORKBOOK_OPEN_START' -Source 'Workbook'
    $wb = $excel.Workbooks.Open($Path, $false, $false) # read/write
    Invoke-ExcelTrace -Trace $Trace -Phase 'WORKBOOK_OPEN_END' -Source 'Workbook'

    if (-not $FastMode) {
      Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_CONFIG_START' -Source 'Workbook'
      $connectionCount = 0
      foreach($cn in @($wb.Connections)){
        $connectionCount++
        try {
          if ($cn.Type -eq 1 -and $cn.ODBCConnection) { $cn.ODBCConnection.BackgroundQuery = $false }  # ODBC
          if ($cn.Type -eq 2 -and $cn.OLEDBConnection){ $cn.OLEDBConnection.BackgroundQuery = $false } # OLEDB
        } catch {
          Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_CONFIG_ITEM_ERROR' -Level 'WARN' -Details "Index=$connectionCount; Error=$($_.Exception.Message)" -Source 'Workbook'
        }
      }
      Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_CONFIG_END' -Details "Count=$connectionCount" -Source 'Workbook'
    }

    # Power Query / Connections
    Invoke-ExcelTrace -Trace $Trace -Phase 'REFRESH_ALL_START' -Source 'Workbook'
    $wb.RefreshAll() | Out-Null
    Invoke-ExcelTrace -Trace $Trace -Phase 'REFRESH_ALL_END' -Source 'Workbook'

    Invoke-ExcelTrace -Trace $Trace -Phase 'ASYNC_QUERY_WAIT_START' -Source 'Workbook'
    try {
      $excel.CalculateUntilAsyncQueriesDone()
      Invoke-ExcelTrace -Trace $Trace -Phase 'ASYNC_QUERY_WAIT_END' -Source 'Workbook'
    } catch {
      Invoke-ExcelTrace -Trace $Trace -Phase 'ASYNC_QUERY_WAIT_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
    }

    Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_WAIT_START' -Details "TimeoutSec=$TimeoutSec" -Source 'Workbook'
    $connectionsFinished = Wait-Connections -wb $wb -TimeoutSec $TimeoutSec -Trace $Trace
    if ($connectionsFinished) {
      Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_WAIT_END' -Source 'Workbook'
    } else {
      # Preserve current behavior: record the timeout but continue to calculation/save.
      Invoke-ExcelTrace -Trace $Trace -Phase 'CONNECTION_WAIT_TIMEOUT' -Level 'WARN' -Details "TimeoutSec=$TimeoutSec; Continuing=True" -Source 'Workbook'
    }

    # Only refresh tables/pivots if present (and not in FastMode)
    if (-not $FastMode){
      Invoke-ExcelTrace -Trace $Trace -Phase 'TABLE_PIVOT_SCAN_START' -Source 'Workbook'
      $hasTables = $false; $hasPivots = $false
      $worksheetCount = 0
      foreach($ws in @($wb.Worksheets)){
        $worksheetCount++
        try { if ($ws.ListObjects().Count -gt 0){ $hasTables = $true } } catch {}
        try { if ($ws.PivotTables().Count -gt 0){ $hasPivots = $true } } catch {}
      }
      Invoke-ExcelTrace -Trace $Trace -Phase 'TABLE_PIVOT_SCAN_END' -Details "Worksheets=$worksheetCount; HasTables=$hasTables; HasPivots=$hasPivots" -Source 'Workbook'
      if ($hasTables){
        foreach($ws in @($wb.Worksheets)){
          foreach($lo in @($ws.ListObjects)){
            try {
              if ($lo.QueryTable -ne $null){
                $sheetName = try { [string]$ws.Name } catch { '<unknown>' }
                $tableName = try { [string]$lo.Name } catch { '<unknown>' }
                Invoke-ExcelTrace -Trace $Trace -Phase 'TABLE_REFRESH_START' -Details "Sheet=$sheetName; Table=$tableName" -Source 'Workbook'
                $lo.Refresh() | Out-Null
                Invoke-ExcelTrace -Trace $Trace -Phase 'TABLE_REFRESH_END' -Details "Sheet=$sheetName; Table=$tableName" -Source 'Workbook'
              }
            } catch {
              Invoke-ExcelTrace -Trace $Trace -Phase 'TABLE_REFRESH_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
            }
          }
        }
      }
      if ($hasPivots){
        foreach($ws in @($wb.Worksheets)){
          foreach($pt in @($ws.PivotTables)){
            try {
              $sheetName = try { [string]$ws.Name } catch { '<unknown>' }
              $pivotName = try { [string]$pt.Name } catch { '<unknown>' }
              Invoke-ExcelTrace -Trace $Trace -Phase 'PIVOT_REFRESH_START' -Details "Sheet=$sheetName; Pivot=$pivotName" -Source 'Workbook'
              $pt.RefreshTable() | Out-Null
              Invoke-ExcelTrace -Trace $Trace -Phase 'PIVOT_REFRESH_END' -Details "Sheet=$sheetName; Pivot=$pivotName" -Source 'Workbook'
            } catch {
              Invoke-ExcelTrace -Trace $Trace -Phase 'PIVOT_REFRESH_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
            }
          }
        }
      }
    }

    Invoke-ExcelTrace -Trace $Trace -Phase 'MODEL_REFRESH_CHECK_START' -Source 'Workbook'
    try {
      if ($wb.Model) {
        Invoke-ExcelTrace -Trace $Trace -Phase 'MODEL_REFRESH_START' -Source 'Workbook'
        $wb.Model.Refresh()
        Start-Sleep -Seconds 5
        Invoke-ExcelTrace -Trace $Trace -Phase 'MODEL_REFRESH_END' -Source 'Workbook'
      } else {
        Invoke-ExcelTrace -Trace $Trace -Phase 'MODEL_REFRESH_SKIP' -Details 'No workbook data model' -Source 'Workbook'
      }
    } catch {
      Invoke-ExcelTrace -Trace $Trace -Phase 'MODEL_REFRESH_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
    }

    Invoke-ExcelTrace -Trace $Trace -Phase 'CALCULATE_FULL_START' -Source 'Workbook'
    try {
      $excel.CalculateFull()
      Invoke-ExcelTrace -Trace $Trace -Phase 'CALCULATE_FULL_END' -Source 'Workbook'
    } catch {
      Invoke-ExcelTrace -Trace $Trace -Phase 'CALCULATE_FULL_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
    }
    # try { $excel.CalculateFullRebuild() } catch {}  # only if needed
    Invoke-ExcelTrace -Trace $Trace -Phase 'SAVE_START' -Source 'Workbook'
    $wb.Save()
    Invoke-ExcelTrace -Trace $Trace -Phase 'SAVE_END' -Source 'Workbook'
  }
  finally {
    if ($wb -ne $null){
      Invoke-ExcelTrace -Trace $Trace -Phase 'WORKBOOK_CLOSE_START' -Source 'Workbook'
      try {
        $wb.Close($true)
        Invoke-ExcelTrace -Trace $Trace -Phase 'WORKBOOK_CLOSE_END' -Source 'Workbook'
      } catch {
        Invoke-ExcelTrace -Trace $Trace -Phase 'WORKBOOK_CLOSE_ERROR' -Level 'WARN' -Details "Error=$($_.Exception.Message)" -Source 'Workbook'
      }
    }
  }
}
