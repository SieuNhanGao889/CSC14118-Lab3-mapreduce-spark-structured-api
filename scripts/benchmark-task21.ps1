[CmdletBinding()]
param(
    [ValidateRange(5, 50)]
    [int]$Runs = 5
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$RuntimeJar = Join-Path $ProjectRoot "target/lab3-1.0.0-all.jar"

if (-not (Test-Path -LiteralPath $RuntimeJar -PathType Leaf)) {
    throw "Runtime JAR not found: $RuntimeJar. Run mvn clean package first."
}

$sparkMasterState = docker inspect --format '{{.State.Status}}' lab3-spark-master 2>$null
if ($LASTEXITCODE -ne 0 -or $sparkMasterState -ne "running") {
    throw "Container lab3-spark-master is not running."
}

docker cp $RuntimeJar lab3-spark-master:/tmp/lab3-all.jar | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Could not copy the runtime JAR to lab3-spark-master."
}

$samples = [System.Collections.Generic.List[double]]::new()
$stageCounts = [System.Collections.Generic.List[int]]::new()
$stageIds = [System.Collections.Generic.List[string]]::new()

for ($run = 1; $run -le $Runs; $run++) {
    Write-Host "[Task 2-1] measured run $run/$Runs"
    $elapsed = $null
    $stageCount = $null
    $ids = $null
    $validationPassed = $false
    $problemLines = [System.Collections.Generic.List[string]]::new()

    $dockerArguments = @(
        "exec", "lab3-spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--conf", "spark.executor.instances=1",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.executor.memory=1g",
        "--conf", "spark.sql.shuffle.partitions=8",
        "--class", "lab3.task21.Task21",
        "/tmp/lab3-all.jar",
        "/workspace/input/amazon_sales.csv",
        "/workspace/output/Task_2-1.parquet"
    )

    # Windows PowerShell 5.1 wraps native stderr (including harmless JVM warnings)
    # as ErrorRecord objects. Keep consuming the stream and judge the process by
    # its exit code plus the explicit validation markers below.
    $savedErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & docker @dockerArguments 2>&1 | ForEach-Object {
            $line = $_.ToString()
            if ($line -match '^TASK21_ELAPSED_SECONDS=([0-9.]+)$') {
                $elapsed = [double]::Parse($Matches[1], [System.Globalization.CultureInfo]::InvariantCulture)
            } elseif ($line -match '^TASK21_FINAL_WRITE_STAGE_COUNT=([0-9]+)$') {
                $stageCount = [int]$Matches[1]
            } elseif ($line -match '^TASK21_FINAL_WRITE_STAGE_IDS=(.*)$') {
                $ids = $Matches[1]
            } elseif ($line -eq 'TASK21_VALIDATION=PASS') {
                $validationPassed = $true
            } elseif ($line -match '(?i)exception|(^|[^a-z])error([^a-z]|$)|failed') {
                $problemLines.Add($line)
            }
        }
        $dockerExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }

    if ($dockerExitCode -ne 0) {
        $problemLines | Select-Object -Last 30 | ForEach-Object { Write-Host $_ }
        throw "Task 2-1 run $run failed with exit code $dockerExitCode."
    }
    if ($null -eq $elapsed -or $null -eq $stageCount -or $null -eq $ids -or -not $validationPassed) {
        throw "Task 2-1 run $run did not emit all required evidence markers."
    }

    $samples.Add($elapsed)
    $stageCounts.Add($stageCount)
    $stageIds.Add($ids)
    Write-Host ("  elapsed={0:F3}s stages={1} ids={2} validation=PASS" -f $elapsed, $stageCount, $ids)
}

$mean = ($samples | Measure-Object -Average).Average
$squaredDeviationSum = 0.0
foreach ($sample in $samples) {
    $squaredDeviationSum += [math]::Pow($sample - $mean, 2)
}
$sampleStandardDeviation = [math]::Sqrt($squaredDeviationSum / ($samples.Count - 1))
$samplesText = ($samples | ForEach-Object {
    $_.ToString("F3", [System.Globalization.CultureInfo]::InvariantCulture)
}) -join ", "

Write-Host ""
Write-Host "Benchmark summary (sample standard deviation)"
Write-Host "| Pipeline | Runs | Samples (s) | Mean (s) | SD (s) | Validation |"
Write-Host "|---|---:|---|---:|---:|---|"
Write-Host ("| Task 2-1 | {0} | {1} | {2:F3} | {3:F3} | PASS (all runs) |" -f $Runs, $samplesText, $mean, $sampleStandardDeviation)
Write-Host "Final-write stage counts: $($stageCounts -join ', ')"
for ($index = 0; $index -lt $stageIds.Count; $index++) {
    Write-Host "Run $($index + 1) final-write stage IDs: $($stageIds[$index])"
}
