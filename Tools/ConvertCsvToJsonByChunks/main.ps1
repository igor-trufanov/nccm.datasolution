[CmdletBinding()]
param (
    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String[]]
    $columnsToSelect,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $sourceFileName,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $sourcePath,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $targetPath,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [ValidateRange(2500, [int]::MaxValue)]
    [Int32]
    $batchSize,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $delimiter,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $chunksTargetFolderName,

    [Parameter(Mandatory = $true, ValueFromPipelineByPropertyName = $true)]
    [String]
    $processName
)

$rowCount = 0
$chunkCount = 0

$targetFileNameTemplate = "{0:yyyy_MM_ddTHH_mm_ss.fffffZ}_chunk_{1}.json"
$datetimeUTC = (Get-Date).ToUniversalTime()
$targetFolder = Join-Path $targetPath $chunksTargetFolderName

if (-Not (Test-Path $targetFolder -PathType Container))
{
    Write-Host "*** Folder doesn't exist. Creating it..."
    Write-Host $targetFolder
    New-Item -ItemType Directory -Force -Path $targetFolder
    Write-Host "*** Done."
} else {
    Write-Host "*** Cleaning target folder..."
    Write-Host $targetFolder
    Remove-Item -Path ($targetFolder + '\*') -Include '*.json' -Force
    Write-Host "*** Done."
}

Write-Host "*** Creating chunks..."
$sourceFileAbsolutePath = Join-Path $sourcePath $sourceFileName
Import-CSV $sourceFileAbsolutePath -Encoding UTF8 -Delimiter $delimiter | Select-Object $columnsToSelect | Foreach-Object { 

    if($rowCount % $batchSize -eq 0) {

        if($stream -ne $null) {  
            $stream.close()
        }

        $targetFileName = $targetFileNameTemplate -f ($datetimeUTC, $chunkCount)
        Write-Host $targetFileName

        $stream = [System.IO.StreamWriter]::new((Join-Path $targetFolder $targetFileName), $false, [Text.Encoding]::Unicode)
        ++$chunkCount
    }

    $json = $_ | ConvertTo-Json -Compress -Depth 1
    $stream.WriteLine($json)

    ++$rowCount
}

if($stream -ne $null) {  
    $stream.close()
}

Write-Host  "*** Done."

exit 0
