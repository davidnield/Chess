# Read-only Windows hardware diagnostics. No repair, dismount, rescan, or settings changes.
$ErrorActionPreference = 'Stop'
$out = 'C:\Users\David\Documents\Chess\scratch\worktrees\sweep-preparation\docs\evidence\f-drive-investigation-20261125'
$result = [ordered]@{ observed_at = (Get-Date -Format o); read_only = $true }
$result.elevated = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
try {
    if (-not $result.elevated) { throw 'Administrator token required for this diagnostic' }
    $disk = Get-Partition -DriveLetter F | Get-Disk
    if ($disk.Number -ne 4 -or $disk.FriendlyName -ne 'Seagate Expansion HDD') { throw 'F drive identity changed' }
    $physical = Get-PhysicalDisk | Where-Object { $_.DeviceId -eq '4' -and $_.FriendlyName -eq 'Seagate Expansion HDD' }
    $result.disk = $disk | Select-Object Number, FriendlyName, BusType, HealthStatus, OperationalStatus
    try {
        $result.reliability = $physical | Get-StorageReliabilityCounter | Select-Object Temperature,TemperatureMax,PowerOnHours,ReadErrorsTotal,ReadErrorsUncorrected,WriteErrorsTotal,WriteErrorsUncorrected,ReadLatencyMax,WriteLatencyMax,Wear
    } catch { $result.reliability_error = $_.Exception.Message }
    try {
        $result.smart_status = @(Get-CimInstance -Namespace root/wmi -ClassName MSStorageDriver_FailurePredictStatus | Select-Object InstanceName,Active,PredictFailure,Reason)
    } catch { $result.smart_error = $_.Exception.Message }
} catch { $result.error = $_.Exception.Message }
$result | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $out 'elevated-hardware-health.json') -Encoding utf8
