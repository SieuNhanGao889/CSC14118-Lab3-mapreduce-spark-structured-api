[CmdletBinding()]
param(
    [ValidateRange(5, 50)]
    [int]$Runs = 5,

    [ValidateSet("all", "task11", "global", "local")]
    [string]$Pipeline = "all"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$RuntimeJar = Join-Path $ProjectRoot "target/lab3-1.0.0-all.jar"

if (-not (Test-Path -LiteralPath $RuntimeJar -PathType Leaf)) {
    throw "Runtime JAR not found: $RuntimeJar. Run mvn clean package first."
}

$nameNodeState = docker inspect --format '{{.State.Status}}' lab3-namenode 2>$null
if ($LASTEXITCODE -ne 0 -or $nameNodeState -ne "running") {
    throw "Container lab3-namenode is not running."
}

docker cp $RuntimeJar lab3-namenode:/tmp/lab3-all.jar | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Could not copy the runtime JAR to lab3-namenode."
}

function Invoke-MeasuredPipeline {
    param(
        [Parameter(Mandatory)]
        [string]$Name,

        [Parameter(Mandatory)]
        [string[]]$HadoopArguments
    )

    $samples = [System.Collections.Generic.List[double]]::new()

    for ($run = 1; $run -le $Runs; $run++) {
        Write-Host "[$Name] measured run $run/$Runs"
        $completionLine = $null
        $problemLines = [System.Collections.Generic.List[string]]::new()
        $dockerArguments = @(
            "exec",
            "lab3-namenode",
            "hadoop",
            "jar",
            "/tmp/lab3-all.jar"
        ) + $HadoopArguments

        & docker @dockerArguments 2>&1 | ForEach-Object {
            $line = $_.ToString()
            if ($line -match "complete in [0-9.]+ seconds") {
                $completionLine = $line
                Write-Host "  $line"
            } elseif ($line -match "(?i)exception|(^|[^a-z])error([^a-z]|$)|failed") {
                $problemLines.Add($line)
            }
        }

        if ($LASTEXITCODE -ne 0) {
            $problemLines | Select-Object -Last 30 | ForEach-Object { Write-Host $_ }
            throw "$Name run $run failed with exit code $LASTEXITCODE."
        }
        if ($null -eq $completionLine -or $completionLine -notmatch "complete in ([0-9.]+) seconds") {
            throw "Could not parse elapsed time for $Name run $run."
        }

        $elapsed = [double]::Parse(
            $Matches[1],
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        $samples.Add($elapsed)
    }

    $mean = ($samples | Measure-Object -Average).Average
    $squaredDeviationSum = 0.0
    foreach ($sample in $samples) {
        $squaredDeviationSum += [math]::Pow($sample - $mean, 2)
    }
    $sampleStandardDeviation = [math]::Sqrt($squaredDeviationSum / ($samples.Count - 1))

    [pscustomobject]@{
        Pipeline = $Name
        Runs = $samples.Count
        SamplesSeconds = ($samples | ForEach-Object {
            $_.ToString("F3", [System.Globalization.CultureInfo]::InvariantCulture)
        }) -join ", "
        MeanSeconds = $mean
        SampleStandardDeviationSeconds = $sampleStandardDeviation
    }
}

Push-Location $ProjectRoot
try {
    $results = @()
    if ($Pipeline -in @("all", "task11")) {
        $results += Invoke-MeasuredPipeline -Name "Task 1-1" -HadoopArguments @(
            "lab3.task11.Task11",
            "/lab3/input/amazon_sales.csv",
            "/lab3/work/task-1-1",
            "/workspace/output/Task_1-1.csv"
        )
    }
    if ($Pipeline -in @("all", "global")) {
        $results += Invoke-MeasuredPipeline -Name "Task 1-2 global" -HadoopArguments @(
            "lab3.task12.Task12",
            "/lab3/input/amazon_sales.csv",
            "/lab3/work/task-1-2-global",
            "/workspace/output/Task_1-2.csv",
            "global"
        )
    }
    if ($Pipeline -in @("all", "local")) {
        $results += Invoke-MeasuredPipeline -Name "Task 1-2 local sensitivity" -HadoopArguments @(
            "lab3.task12.Task12",
            "/lab3/input/amazon_sales.csv",
            "/lab3/work/task-1-2-local",
            "/workspace/output/Task_1-2-local-sensitivity.csv",
            "local"
        )
    }

    Write-Host ""
    Write-Host "Benchmark summary (sample standard deviation)"
    Write-Host "| Pipeline | Runs | Samples (s) | Mean (s) | SD (s) |"
    Write-Host "|---|---:|---|---:|---:|"
    foreach ($result in $results) {
        $meanText = $result.MeanSeconds.ToString(
            "F3",
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        $sdText = $result.SampleStandardDeviationSeconds.ToString(
            "F3",
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        Write-Host "| $($result.Pipeline) | $($result.Runs) | $($result.SamplesSeconds) | $meanText | $sdText |"
    }
}
finally {
    Pop-Location
}
