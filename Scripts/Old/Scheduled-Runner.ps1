# Simple-Excel-Runner.ps1
param(
    [string[]]$MasterFiles = @(
        "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\01 Data Update - 11.00 PM.xlsx",
        "\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\02 Data Update - 05.00 AM.xlsx"
    ),
    [string[]]$BatchNames = @("23:00", "05:00")
)

$logDir = "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo"
$logFile = Join-Path $logDir "simple-runner-log.txt"

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] $Message"
    Write-Host $logEntry
    $logEntry | Out-File $logFile -Append
}

function Cleanup-Excel {
    try {
        Write-Log "Cleaning up Excel processes..."
        Get-Process -Name "EXCEL" -ErrorAction SilentlyContinue | Stop-Process -Force
        taskkill /f /im excel.exe /t 2>&1 | Out-Null
        Start-Sleep -Seconds 2
        [System.GC]::Collect()
        [System.GC]::WaitForPendingFinalizers()
    }
    catch {
        Write-Log "Cleanup warning: $($_.Exception.Message)"
    }
}

function Process-MasterFile {
    param(
        [string]$MasterPath,
        [string]$BatchName,
        [int]$FileIndex
    )
    
    $originalFileName = Split-Path $MasterPath -Leaf
    Write-Log "Processing file $($FileIndex + 1): $originalFileName"
    Write-Log "Batch name: $BatchName"
    
    $localTempDir = "C:\Temp\ExcelAutomation"
    $localFilePath = Join-Path $localTempDir $originalFileName
    
    try {
        # Clean up before processing
        Cleanup-Excel
        
        # Ensure temp directory exists
        if (-not (Test-Path $localTempDir)) {
            New-Item -ItemType Directory -Path $localTempDir -Force | Out-Null
        }
        
        # Clean up any existing temp file
        if (Test-Path $localFilePath) {
            Remove-Item $localFilePath -Force -ErrorAction SilentlyContinue
        }
        
        # Copy file locally
        Write-Log "Copying file to local temp location..."
        Copy-Item $MasterPath -Destination $localFilePath -Force
        
        if (-not (Test-Path $localFilePath)) {
            throw "Failed to copy file to local location"
        }
        
        Write-Log "Local copy created: $localFilePath"
        
        # Run the main script with local file
        Set-Location "C:\Users\kapl\Desktop\Project-Reporting-Automation\Scripts"
        
        .\Run-Parallel.ps1 -MasterPath $localFilePath `
                          -SheetName "" `
                          -PathColumn "B" `
                          -StartRow 2 `
                          -LogPath "..\Logginfo\run-log.csv" `
                          -ThrottleLimit 3 `
                          -Batch $BatchName
        
        Write-Log "Successfully processed file $($FileIndex + 1)"
        return $true
    }
    catch {
        Write-Log "Error processing file $($FileIndex + 1): $($_.Exception.Message)"
        return $false
    }
    finally {
        # Clean up temp file
        try {
            if (Test-Path $localFilePath) {
                Remove-Item $localFilePath -Force -ErrorAction SilentlyContinue
                Write-Log "Cleaned up temp file: $localFilePath"
            }
        }
        catch {
            Write-Log "Warning: Could not clean up temp file: $($_.Exception.Message)"
        }
        
        # Clean up Excel after each file
        Cleanup-Excel
    }
}

# Main execution
try {
    Write-Log "=== Simple Excel Runner Started ==="
    Write-Log "User: $env:USERNAME, Computer: $env:COMPUTERNAME"
    Write-Log "Processing $($MasterFiles.Count) master file(s)"
    
    # Validate input
    if ($MasterFiles.Count -ne $BatchNames.Count) {
        throw "Number of master files ($($MasterFiles.Count)) does not match number of batch names ($($BatchNames.Count))"
    }
    
    $successCount = 0
    $failedCount = 0
    
    # Process each master file in sequence
    for ($i = 0; $i -lt $MasterFiles.Count; $i++) {
        Write-Log "--- Starting file $($i + 1) of $($MasterFiles.Count) ---"
        
        if (Process-MasterFile -MasterPath $MasterFiles[$i] -BatchName $BatchNames[$i] -FileIndex $i) {
            $successCount++
        } else {
            $failedCount++
        }
        
        Write-Log "--- Completed file $($i + 1) ---"
        Write-Log ""
    }
    
    # Final summary
    Write-Log "=== Processing Complete ==="
    Write-Log "Successful: $successCount, Failed: $failedCount, Total: $($MasterFiles.Count)"
    
    if ($failedCount -eq 0) {
        Write-Log "=== ALL FILES PROCESSED SUCCESSFULLY ==="
        exit 0
    } else {
        Write-Log "=== COMPLETED WITH $failedCount FAILURE(S) ==="
        exit 1
    }
}
catch {
    Write-Log "CRITICAL ERROR: $($_.Exception.Message)"
    Write-Log "Stack trace: $($_.Exception.StackTrace)"
    exit 1
}
finally {
    # Final cleanup
    Cleanup-Excel
    Write-Log "Script execution completed at $(Get-Date)"
}