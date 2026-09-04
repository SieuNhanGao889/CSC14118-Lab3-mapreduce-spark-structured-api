[CmdletBinding()]
param(
    [switch]$SkipSetup
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$SetupScript = Join-Path $ProjectRoot "setup-lab3.ps1"
$ThinJar = Join-Path $ProjectRoot "target/lab3.jar"
$RuntimeJar = Join-Path $ProjectRoot "target/lab3-1.0.0-all.jar"
$InputCsv = "/workspace/input/amazon_sales.csv"
$Profile = "/workspace/output/data-profile.md"
$HdfsInput = "/lab3/input/amazon_sales.csv"
$HdfsSmokeOutput = "/lab3/smoke/mapreduce-record-count"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Assert-LastCommand {
    param([string]$Message)
    if ($LASTEXITCODE -ne 0) {
        throw "$Message (exit code: $LASTEXITCODE)"
    }
}

Push-Location $ProjectRoot
try {
    if (-not $SkipSetup) {
        Write-Step "Validating services and HDFS input"
        & $SetupScript -SkipPull
    }

    Write-Step "Compiling Scala preparation utilities"
    docker compose --profile tools run --rm build mvn --batch-mode --no-transfer-progress clean package
    Assert-LastCommand "Maven build failed"
    if (-not (Test-Path -LiteralPath $ThinJar) -or -not (Test-Path -LiteralPath $RuntimeJar)) {
        throw "Expected JAR artifacts were not produced."
    }

    Write-Step "Running a real MapReduce job through YARN"
    docker cp $RuntimeJar lab3-namenode:/tmp/lab3-all.jar
    Assert-LastCommand "Could not copy the MapReduce runtime JAR"
    docker exec lab3-namenode hdfs dfs -test -e $HdfsInput
    Assert-LastCommand "HDFS input is missing; run setup-lab3.ps1 without -SkipDataUpload"
    docker exec lab3-namenode hdfs dfs -rm -r -f $HdfsSmokeOutput | Out-Null
    docker exec lab3-namenode hadoop jar /tmp/lab3-all.jar lab3.tools.MapReduceSmoke $HdfsInput $HdfsSmokeOutput
    Assert-LastCommand "MapReduce smoke job failed"
    $MapReduceResult = docker exec lab3-namenode hdfs dfs -cat "$HdfsSmokeOutput/part-r-00000"
    Assert-LastCommand "Could not read MapReduce smoke output"
    if ($MapReduceResult -notmatch '^physical_lines\s+128976$') {
        throw "Unexpected MapReduce result: $MapReduceResult"
    }
    Write-Host "MapReduce result: $MapReduceResult" -ForegroundColor Green

    Write-Step "Running distributed Spark CSV/Parquet round-trip"
    docker cp $ThinJar lab3-spark-master:/tmp/lab3.jar
    Assert-LastCommand "Could not copy the Spark JAR"
    docker exec lab3-spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.executor.instances=1 `
        --conf spark.executor.cores=2 `
        --conf spark.executor.memory=1g `
        --class lab3.tools.SparkIoSmoke `
        /tmp/lab3.jar `
        $InputCsv `
        /workspace/output/smoke-csv `
        /workspace/output/smoke-parquet
    Assert-LastCommand "Spark I/O smoke job failed"

    Write-Step "Generating the reproducible Spark data profile"
    docker exec lab3-spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.executor.instances=1 `
        --conf spark.executor.cores=2 `
        --conf spark.executor.memory=1g `
        --class lab3.tools.DataProfiler `
        /tmp/lab3.jar `
        $InputCsv `
        $Profile
    Assert-LastCommand "Spark data profiling failed"

    $HostProfile = Join-Path $ProjectRoot "output/data-profile.md"
    if (-not (Test-Path -LiteralPath $HostProfile -PathType Leaf)) {
        throw "Data profile was not written to $HostProfile"
    }

    Write-Host "`nPreparation verification passed." -ForegroundColor Green
    Write-Host "Profile: $HostProfile"
    Write-Host "MapReduce evidence: $HdfsSmokeOutput"
}
finally {
    Pop-Location
}
