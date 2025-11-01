<# ================== Shared-Excel-Helpers.ps1 ==================
Utilities:
- Start/Stop Excel (hidden)
- Wait for connections to finish
- Read master (B path, E new path, F method) → returns objects {Path, Method, Row}
- Refresh-WorkbookSmart (PQ then tables/pivots-if-present, FastMode optional)
#>

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

function Refresh-WorkbookSmart([object]$excel,[string]$Path,[int]$TimeoutSec=900,[switch]$FastMode){
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
