@echo off
echo [%date% %time%] Task started >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"

:: Change to script directory
cd /d "C:\Users\kapl\Desktop\Project-Reporting-Automation\Scripts"
echo Working directory: %CD% >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"

:: Test PowerShell execution first
echo Testing basic PowerShell execution... >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
"C:\Program Files\PowerShell\7\pwsh.exe" -Command "Get-Date | Out-File 'C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\ps-test.txt' -Append"
if %errorlevel% equ 0 (
    echo Basic PowerShell test passed >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
) else (
    echo Basic PowerShell test failed with error %errorlevel% >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
    exit 1
)


:: Execute the actual script with detailed error capture
echo Launching main PowerShell script... >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
"C:\Program Files\PowerShell\7\pwsh.exe" -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "& { 
    try {
        .\Run-Parallel.ps1 -MasterPath '\\192.168.1.237\Accounts\SURESH_KAKEE_AUTOMATION PROJECTS\Automation_Process\01_Data_Update-11.00PMV2.xlsx' -SheetName '' -PathColumn 'B' -StartRow 2 -LogPath '..\Logginfo\run-log.csv' -ThrottleLimit 3 -Batch '23:00'
        exit 0
    }
    catch {
        Write-Error $_.Exception.Message
        exit 1
    }
}"

:: Capture the exact error code
set PS_ERROR=%errorlevel%
if %PS_ERROR% equ 0 (
    echo [%date% %time%] PowerShell executed successfully >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
) else (
    echo [%date% %time%] PowerShell failed with error code %PS_ERROR% >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"
)

echo [%date% %time%] Task completed >> "C:\Users\kapl\Desktop\Project-Reporting-Automation\Logginfo\task-log.txt"