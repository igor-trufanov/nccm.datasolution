$numberOfProcesses = 0
$templateString = "*PROCESS_UPLOADING_NCCM_RUNNING*"

while ($true) {
    $numberOfProcesses = (Get-WmiObject win32_process | Where-Object {$_.CommandLine -ilike $templateString}).count

    if ($numberOfProcesses -eq 0) {
        break
    }

    Write-Host "*** Keep checking... waiting 30 seconds "
    Start-Sleep -Seconds 30
}

exit 0