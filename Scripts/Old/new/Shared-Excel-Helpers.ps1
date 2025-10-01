<# ================== Shared-Excel-Helpers.ps1 ==================
Utilities for:
- Start/Stop Excel (hidden)
- Get file paths from a master workbook (column, row range)
- Wait for Power Query/Connections to finish
- Smart refresh: PQ first, then only refresh tables/pivots if present
#>

function Start-Excel {
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false
  # Speed tweaks
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

function Wait-Connections($wb, [int]$TimeoutSec=600){
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

function Get-PathsFromMaster([string]$MasterPath,[string]$SheetName,[string]$PathColumn,[int]$StartRow,[int]$EndRow){
  if (-not (Test-Path $MasterPath)){ throw "Master file not found: $MasterPath" }
  $excel = Start-Excel
  $wb = $null
  try {
    $wb = $excel.Workbooks.Open($MasterPath, $false, $true)  # ReadOnly
    $ws = if ([string]::IsNullOrWhiteSpace($SheetName)) { $wb.Worksheets.Item(1) } else { $wb.Worksheets.Item($SheetName) }
    $last = if ($EndRow -gt 0) { $EndRow } else { $ws.Cells($ws.Rows.Count, $PathColumn).End(-4162).Row } # xlUp

    $list = New-Object System.Collections.Generic.List[string]
    for($r=$StartRow; $r -le $last; $r++){
      $v = [string]$ws.Range("$PathColumn$r").Value()
      if (-not [string]::IsNullOrWhiteSpace($v)) { $list.Add($v.Trim()) }
    }
    return ,$list.ToArray()
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
    $wb = $excel.Workbooks.Open($Path, $false, $false) # UpdateLinks:=0, ReadOnly:=False

    # FastMode: allow PQ to parallelize (don’t force BackgroundQuery=False)
    if (-not $FastMode) {
      foreach($cn in @($wb.Connections)){
        try {
          if ($cn.Type -eq 1 -and $cn.ODBCConnection) { $cn.ODBCConnection.BackgroundQuery = $false }  # ODBC
          if ($cn.Type -eq 2 -and $cn.OLEDBConnection){ $cn.OLEDBConnection.BackgroundQuery = $false } # OLEDB
        } catch {}
      }
    }

    # 1) Power Query/Connections
    $wb.RefreshAll() | Out-Null
    [void](Wait-Connections -wb $wb -TimeoutSec $TimeoutSec)

    # 2) Only refresh tables/pivots if present
    $hasTables = $false; $hasPivots = $false
    foreach($ws in @($wb.Worksheets)){
      try { if ($ws.ListObjects().Count -gt 0){ $hasTables = $true } } catch {}
      try { if ($ws.PivotTables().Count -gt 0){ $hasPivots = $true } } catch {}
    }

    if (-not $FastMode -and $hasTables){
      foreach($ws in @($wb.Worksheets)){
        foreach($lo in @($ws.ListObjects)){
          try {
            if ($lo.QueryTable -ne $null){ $lo.Refresh() | Out-Null }
          } catch {}
        }
      }
    }

    if (-not $FastMode -and $hasPivots){
      foreach($ws in @($wb.Worksheets)){
        foreach($pt in @($ws.PivotTables)){
          try { $pt.RefreshTable() | Out-Null } catch {}
        }
      }
    }

    $wb.Save()
  }
  finally {
    if ($wb -ne $null){ try { $wb.Close($true) } catch {} }
  }
}
