param(
    [switch]$SkipUp
)

$ErrorActionPreference = "Stop"

# Documentacion de uso: ver README.md, seccion "Smoke test automatizado".

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$ErrorMessage
    )

    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        throw $ErrorMessage
    }
}

function Wait-ServiceHealthy {
    param(
        [Parameter(Mandatory = $true)][string]$ServiceName,
        [int]$MaxAttempts = 30,
        [int]$SleepSeconds = 2
    )

    for ($i = 1; $i -le $MaxAttempts; $i++) {
        $runningContainers = docker ps --format "{{.Names}}"
        if ($LASTEXITCODE -ne 0) {
            throw "No se pudo consultar el estado de Docker."
        }

        if ($runningContainers -contains $ServiceName) {
            $status = docker inspect --format "{{.State.Health.Status}}" $ServiceName 2>$null
        } else {
            $status = $null
        }

        if ($status -eq "healthy") {
            Write-Host "$ServiceName healthy."
            return
        }

        if ($i -eq $MaxAttempts) {
            throw "Timeout esperando a '$ServiceName' en estado healthy."
        }

        Start-Sleep -Seconds $SleepSeconds
    }
}

Write-Step "Directorio de trabajo: Proyecto_Imagenes_IA"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not $SkipUp) {
    Write-Step "Levantando infraestructura minima"
    Invoke-Checked -Command "docker compose up -d" -ErrorMessage "docker compose up -d fallo."
} else {
    Write-Step "Se omite docker compose up (-SkipUp)"
}

Write-Step "Esperando healthchecks de Kafka, Postgres y MinIO"
Wait-ServiceHealthy -ServiceName "kafka"
Wait-ServiceHealthy -ServiceName "postgres"
Wait-ServiceHealthy -ServiceName "minio"

Write-Step "Verificando topics Kafka"
# Topics reales usados por el pipeline (images.raw y cmd.storage fueron desestimados:
# API1 actua directamente como productor de cmd.face_detection sin topic intermedio).
$expectedTopics = @(
    "evt.face_detection.completed",
    "evt.age_detection.completed",
    "evt.pixelation.completed",
    "evt.storage.completed",
    "cmd.face_detection",
    "cmd.age_detection",
    "cmd.pixelation",
    "cmd.realtime.classification",
    "evt.realtime.classification.completed",
    "events.dead_letter"
)

function Get-KafkaTopics {
    $result = docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudieron listar topics de Kafka."
    }
    return $result
}

function New-KafkaTopicIfMissing {
    param([Parameter(Mandatory = $true)][string]$TopicName)

    docker exec kafka kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists --topic $TopicName --partitions 1 --replication-factor 1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo crear el topic '$TopicName'."
    }
}

$topics = Get-KafkaTopics
$missing = @($expectedTopics | Where-Object { $topics -notcontains $_ })

if ($missing.Count -gt 0) {
    Write-Host ("Faltan topics, creando automaticamente: " + ($missing -join ", ")) -ForegroundColor Yellow
    foreach ($topic in $missing) {
        New-KafkaTopicIfMissing -TopicName $topic
    }

    $topics = Get-KafkaTopics
    $missing = @($expectedTopics | Where-Object { $topics -notcontains $_ })
    if ($missing.Count -gt 0) {
        throw ("No se pudieron asegurar todos los topics. Faltan: " + ($missing -join ", "))
    }
}
Write-Host "Topics OK."

Write-Step "Verificando conectividad Kafka (produce/consume en events.dead_letter)"
$pingMsg = '{"smoke":"ping"}'
$pingMsg | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic events.dead_letter | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo publicar mensaje de prueba en Kafka."
}
Write-Host "Kafka produce OK."

Write-Step "Comprobando MinIO (health endpoint)"
docker exec minio curl -fsS http://localhost:9000/minio/health/live | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "MinIO no responde correctamente."
}
Write-Host "MinIO OK."

Write-Step "Comprobando Postgres (consulta simple)"
docker exec postgres psql -U bda_user -d bda_imagenes -c "SELECT 1;" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Postgres no responde correctamente."
}
Write-Host "Postgres OK."

Write-Host ""
Write-Host "SMOKE TEST OK: infraestructura lista para trabajar con scripts y consola." -ForegroundColor Green
