param(
    [Parameter(Mandatory=$true)]
    [string]$BatchNumbers,                # e.g. "1,2" or "3"
    [switch]$FastMode,
    [string]$DbConn = $env:REPORTLOGS_CONN,  # prefer env var
    [string]$ScriptsDir = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Scripts",
    [string]$LogDir    = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo",
    [string]$TempDir   = "C:\Temp\ExcelAutomation"
)

# ---- Validate DB conn ----
if (-not $DbConn) {
    throw "No DB connection string provided. Set -DbConn or define environment variable REPORTLOGS_CONN."
}

# ---- Batch map (extend as needed) ----
$MasterFileMap = @{
    1 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\01 Data Update - 11.00 PM.xlsx"
    2 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\02 Data Update - 05.00 AM.xlsx"
    3 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\03 Data Update - 11.00 AM.xlsx"
    4 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\04 Data Update - 12.00 PM.xlsx"
    5 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\05 Data Update - 01.30 PM.xlsx"
    6 = "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\06 Data Update - 02.00 PM.xlsx"
}
$BatchNameMap = @{
    1 = "23:00"
    2 = "05:00"
    3 = "11:00"
    4 = "12:00"
    5 = "13:30"
    6 = "14:00"
}

# ---- Parse BatchNumbers into array of ints ----
$BatchArray = @()
foreach ($num in $BatchNumbers.Split(',')) {
    $t = $num.Trim()
    if ($t -match '^\d+$') { $BatchArray += [int]$t } else { Write-Warning "Skipping invalid batch number: '$num'" }
}
if ($BatchArray.Count -eq 0) { throw "No valid batch numbers provided." }

# ---- Ensure folders ----
New-Item -ItemType Directory -Force -Path $LogDir  | Out-Null
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

# ---- Light logger ----
$RunnerLog = Join-Path $LogDir "scheduled-runner.log"
function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Write-Host $line
    $line | Out-File $RunnerLog -Append -Encoding utf8
}

# ---- Excel cleanup (unchanged) ----
function Cleanup-Excel {
    try {
        Write-Log "Cleaning up lingering Excel…"
        Get-Process -Name "EXCEL" -ErrorAction SilentlyContinue | Stop-Process -Force
        cmd /c "taskkill /f /im excel.exe /t" | Out-Null
        Start-Sleep -Seconds 2
        [System.GC]::Collect(); [System.GC]::WaitForPendingFinalizers()
    } catch { Write-Log "Cleanup warning: $($_.Exception.Message)" }
}

# ---- Load MySql.Data (Connector/NET) ----
# Adjust path to the DLL if needed (you installed Connector/NET 9.4)
try {
    Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
} catch {
    Write-Log "Could not load MySql.Data from GAC, trying a direct path…"
    # fallback path example (adjust if needed):
    $dllGuess = "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.4\MySql.Data.dll"
    if (Test-Path $dllGuess) {
        Add-Type -Path $dllGuess
    } else {
        throw "MySql.Data not found. Install MySQL Connector/NET or update the DLL path."
    }
}

# ---- Export one run_id to CSV (same columns/format as before) ----
function Export-RunLogCsv {
    param(
        [Parameter(Mandatory=$true)][string]$RunId,
        [Parameter(Mandatory=$true)][string]$OutCsv,
        [Parameter(Mandatory=$true)][string]$DbConn
    )
    Write-Log "Exporting run_id '$RunId' to $OutCsv …"
    try {
        $conn = [MySql.Data.MySqlClient.MySqlConnection]::new($DbConn)
        $conn.Open()
        $sql = @"
SELECT
  DATE_FORMAT(CONVERT_TZ(timestamp_utc,'+00:00','+05:30'), '%Y-%m-%d %H:%i:%s') AS Timestamp,
  DATE_FORMAT(rundate, '%Y-%m-%d')               AS RunDate,
  batch                                          AS Batch,
  stage                                          AS Stage,
  master_path                                    AS Master,
  file_path                                      AS FilePath,
  method                                         AS Method,
  status                                         AS Status,
  error_text                                     AS Error,
  duration_s                                     AS DurationS,
  recipients_to                                  AS RecipientsTo,
  subject                                        AS Subject
FROM events
WHERE run_id = @run
ORDER BY timestamp_utc ASC, id ASC
"@
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $sql
        $p = $cmd.Parameters
        $null = $p.Add("@run",[MySql.Data.MySqlClient.MySqlDbType]::VarChar).Value = $RunId

        $ad = New-Object MySql.Data.MySqlClient.MySqlDataAdapter $cmd
        $dt = New-Object System.Data.DataTable
        [void]$ad.Fill($dt)
        $conn.Close()

        # Ensure directory exists
        New-Item -ItemType Directory -Force -Path (Split-Path $OutCsv -Parent) | Out-Null

        # Export CSV with same header order as your existing files
        $cols = "Timestamp","RunDate","Batch","Stage","Master","FilePath","Method","Status","Error","DurationS","RecipientsTo","Subject"
        $dt | Select-Object $cols | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
        Write-Log "Exported $($dt.Rows.Count) rows."
        return $true
    } catch {
        Write-Log "Export failed: $($_.Exception.Message)"
        return $false
    } finally {
        if ($conn -and $conn.State -ne 'Closed') { $conn.Close() }
        if ($conn) { $conn.Dispose() }
    }
}

# ---- Build LogIdentifier (run_id) with your 6 PM rule for night batches ----
function Get-RunId {
    param(
        [int]$BatchNumber,
        [datetime]$NowLocal = (Get-Date)
    )
    # Night windows example: batches 1 & 2 roll filename date after 18:00
    $fileDate = if ($BatchNumber -in @(1,2) -and $NowLocal.Hour -ge 18) { $NowLocal.AddDays(1) } else { $NowLocal }
    $dateStr  = $fileDate.ToString("yyyy-MM-dd")
    return "run-log_{0}_Batch-{1}" -f $dateStr, $BatchNumber
}

# ---- Main ----
Write-Log "=== Scheduled-Runner started ==="
Write-Log "User=$env:USERNAME  Computer=$env:COMPUTERNAME  FastMode=$($FastMode.IsPresent)"
Write-Log "Batches: $BatchNumbers"

$success = 0; $failed = 0

foreach ($bn in $BatchArray) {
    if (-not $MasterFileMap.ContainsKey($bn)) {
        Write-Log "WARNING: Unknown batch $bn — skipping."
        continue
    }

    $masterPath  = $MasterFileMap[$bn]
    $batchName   = $BatchNameMap[$bn]
    $runId       = Get-RunId -BatchNumber $bn
    $outCsv      = Join-Path $LogDir "$runId.csv"    # same file name pattern as before
    $localCopy   = Join-Path $TempDir (Split-Path $masterPath -Leaf)

    Write-Log "---- Batch $bn ($batchName) ----"
    Write-Log "Master: $masterPath"
    Write-Log "RunId : $runId"

    try {
        #Cleanup-Excel

        if (Test-Path $localCopy) {
            Remove-Item $localCopy -Force -ErrorAction SilentlyContinue
        }
        Copy-Item $masterPath -Destination $localCopy -Force
        if (-not (Test-Path $localCopy)) { throw "Failed to copy master locally." }
        Write-Log "Local copy: $localCopy"

        Push-Location $ScriptsDir
        try {
            # Call your existing Run-Parallel.ps1
            # NOTE: We pass LogIdentifier (for continuity) AND DbConn (new). Run-Parallel will be updated next to use DB.
            $args = @{
                MasterPath   = $localCopy
                SheetName    = ""
                PathColumn   = "B"
                StartRow     = 2
                ThrottleLimit= 3
                Batch        = $batchName
                LogIdentifier= $runId
                FastMode     = $FastMode.IsPresent
                DbConn       = $DbConn        # <-- Run-Parallel will start using this after we update it
            }
            Write-Log "Invoking Run-Parallel.ps1 …"
            .\Run-Parallel.ps1 @args
            Write-Log "Run-Parallel.ps1 finished."
        } finally {
            Pop-Location
        }

        # Export the consolidated CSV for this run_id from DB (so email step can keep using CSV)
        if (Export-RunLogCsv -RunId $runId -OutCsv $outCsv -DbConn $DbConn) {
            Write-Log "Batch $bn CSV exported: $outCsv"
        } else {
            Write-Log "Batch $bn CSV export FAILED (DB issue)."
        }

        $success++
    } catch {
        Write-Log "ERROR batch $bn : $($_.Exception.Message)"
        $failed++
    } finally {
        # Cleanup local copy
        try {
            if (Test-Path $localCopy) { Remove-Item $localCopy -Force -ErrorAction SilentlyContinue }
        } catch { Write-Log "Warning removing local copy: $($_.Exception.Message)" }

        #Cleanup-Excel
        Start-Sleep -Seconds 2
    }
}

Write-Log "=== Summary: OK=$success  FAIL=$failed  Total=$($BatchArray.Count) ==="
if ($failed -gt 0) { exit 1 } else { exit 0 }
