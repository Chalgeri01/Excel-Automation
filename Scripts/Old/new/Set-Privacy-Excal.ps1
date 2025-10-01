# Registry paths for Office versions
$officeVersions = @("15.0", "16.0")
$regBasePath = "HKCU:\Software\Microsoft\Office"

foreach ($ver in $officeVersions) {
    $regPath = "$regBasePath\$ver\Common\Privacy"
    
    # Ensure the path exists
    if (-not (Test-Path $regPath)) {
        New-Item -Path $regPath -Force | Out-Null
    }

    # Set the privacy option (2 = Always ignore Privacy Level settings)
    Set-ItemProperty -Path $regPath -Name "PrivacyLevel" -Value 2 -Type DWord

    Write-Host "PrivacyLevel set for Office $ver"
}
