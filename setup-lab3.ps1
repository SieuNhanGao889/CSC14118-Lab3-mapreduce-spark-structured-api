[CmdletBinding()]
param(
    [switch]$SkipPull,
    [switch]$SkipDataUpload
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
$DocsDirectory = Join-Path $ProjectRoot "docs"
$EvidenceDirectory = Join-Path $DocsDirectory "evidence"
$LogFile = Join-Path $EvidenceDirectory "environment-check.txt"

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

function Wait-HealthyContainer {
    param(
        [string]$ContainerName,
        [int]$TimeoutSeconds = 240
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $State = docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ContainerName 2>$null
        if ($LASTEXITCODE -eq 0 -and $State -eq "healthy") {
            Write-Host "$ContainerName is healthy." -ForegroundColor Green
            return
        }
        if ($State -eq "unhealthy" -or $State -eq "exited" -or $State -eq "dead") {
            docker logs --tail 80 $ContainerName
            throw "$ContainerName entered state '$State'."
        }
        Start-Sleep -Seconds 5
    }

    docker logs --tail 80 $ContainerName
    throw "Timed out waiting for $ContainerName to become healthy."
}

function Wait-HdfsWritable {
    param([int]$TimeoutSeconds = 180)

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $SafeModeState = docker exec lab3-namenode hdfs dfsadmin -safemode get 2>$null
        if ($LASTEXITCODE -eq 0 -and $SafeModeState -match "Safe mode is OFF") {
            Write-Host "HDFS is writable (safe mode is OFF)." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 3
    }

    throw "Timed out waiting for HDFS to leave safe mode."
}

if (-not (Test-Path $ComposeFile)) {
    throw "docker-compose.yml was not found beside setup-lab3.ps1."
}

foreach ($RelativeDirectory in @("data/raw", "data/test", "src", "output", "docs", "docs/evidence")) {
    New-Item -ItemType Directory -Force (Join-Path $ProjectRoot $RelativeDirectory) | Out-Null
}

Push-Location $ProjectRoot
Start-Transcript -Path $LogFile -Force | Out-Null

try {
    Write-Step "Checking Docker Desktop"
    docker version
    Assert-LastCommand "Docker is unavailable. Start Docker Desktop and retry"

    Write-Step "Validating docker-compose.yml"
    docker compose -f $ComposeFile config --quiet
    Assert-LastCommand "Docker Compose validation failed"

    if (-not $SkipPull) {
        Write-Step "Pulling pinned Hadoop, Spark, and Maven images"
        docker compose -f $ComposeFile --profile tools pull
        Assert-LastCommand "One or more container images could not be pulled"
    }

    Write-Step "Starting the Lab 3 stack"
    docker compose -f $ComposeFile up -d
    Assert-LastCommand "The Lab 3 stack could not be started"

    Write-Step "Waiting for core services"
    Wait-HealthyContainer "lab3-namenode"
    Wait-HealthyContainer "lab3-resourcemanager"
    Wait-HealthyContainer "lab3-spark-master"

    Write-Step "Showing container status"
    docker compose -f $ComposeFile ps
    Assert-LastCommand "Could not read container status"

    Write-Step "Recording Hadoop version"
    docker exec lab3-namenode hadoop version
    Assert-LastCommand "Hadoop version check failed"

    Write-Step "Checking HDFS"
    docker exec lab3-namenode hdfs dfsadmin -report
    Assert-LastCommand "HDFS health check failed"
    Wait-HdfsWritable

    if (-not $SkipDataUpload) {
        $InputFile = Join-Path $ProjectRoot "data/raw/Amazon Sale Report.csv"
        if (-not (Test-Path -LiteralPath $InputFile -PathType Leaf)) {
            throw "Required input dataset was not found: $InputFile"
        }

        Write-Step "Uploading the immutable source dataset to HDFS"
        docker exec lab3-namenode hdfs dfs -mkdir -p /lab3/input
        Assert-LastCommand "Could not create the HDFS input directory"
        docker exec lab3-namenode hdfs dfs -put -f /workspace/input/amazon_sales.csv /lab3/input/amazon_sales.csv
        Assert-LastCommand "Could not upload the source dataset to HDFS"
        docker exec lab3-namenode hdfs dfs -ls -h /lab3/input/amazon_sales.csv
        Assert-LastCommand "Could not verify the HDFS input dataset"
    }

    Write-Step "Checking YARN"
    docker exec lab3-resourcemanager yarn node -list
    Assert-LastCommand "YARN health check failed"

    Write-Step "Recording Spark and Scala versions"
    docker exec lab3-spark-master /opt/spark/bin/spark-submit --version
    Assert-LastCommand "Spark version check failed"

    Write-Step "Recording Maven and Java build versions"
    docker compose -f $ComposeFile --profile tools run --rm build mvn --version
    Assert-LastCommand "Build-container check failed"

    Write-Host "`nLab 3 environment is ready." -ForegroundColor Green
    Write-Host "HDFS UI:        http://localhost:9871"
    Write-Host "YARN UI:        http://localhost:8089"
    Write-Host "Spark Master UI: http://localhost:8080"
    Write-Host "Spark Worker UI: http://localhost:8081"
    Write-Host "Environment log: $LogFile"
}
finally {
    Stop-Transcript | Out-Null
    Pop-Location
}
