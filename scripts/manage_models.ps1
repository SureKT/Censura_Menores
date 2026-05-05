<#
.SYNOPSIS
    Gestiona las versiones del modelo de edad.

.DESCRIPTION
    Comandos disponibles:
      list          Lista todas las versiones registradas.
      use <id>      Activa una version (copia al modelo en uso y reinicia contenedores).
      info <id>     Muestra los detalles de una version.

.EXAMPLE
    .\manage_models.ps1 list
    .\manage_models.ps1 use v1
    .\manage_models.ps1 info v2
#>

param(
    [Parameter(Position = 0)][string]$Command = "list",
    [Parameter(Position = 1)][string]$Version  = ""
)

$ErrorActionPreference = "Stop"

$ProjectRoot  = Join-Path $PSScriptRoot ".."
$ModelsDir    = Join-Path $ProjectRoot "services\age_detection\models"
$RegistryPath = Join-Path $ModelsDir   "registry.json"
$ActivePath   = Join-Path $ProjectRoot "services\age_detection\age_model.pth"


# ── Helpers ───────────────────────────────────────────────────────────────────

function Get-Registry {
    if (-not (Test-Path $RegistryPath)) {
        return [PSCustomObject]@{ versions = @() }
    }
    return Get-Content $RegistryPath -Raw | ConvertFrom-Json
}

function Save-Registry($registry) {
    # Serializar preservando arrays aunque tengan un solo elemento
    $json = $registry | ConvertTo-Json -Depth 6
    Set-Content -Path $RegistryPath -Value $json -Encoding utf8
}

function Find-Version($registry, $id) {
    return $registry.versions | Where-Object { $_.id -eq $id } | Select-Object -First 1
}

function Get-ActiveId($registry) {
    $active = $registry.versions | Where-Object { $_.active -eq $true } | Select-Object -First 1
    if ($active) { return $active.id } else { return "(ninguna)" }
}


# ── Comando: list ─────────────────────────────────────────────────────────────

function Invoke-List {
    $registry = Get-Registry
    if ($registry.versions.Count -eq 0) {
        Write-Host "No hay versiones registradas aun." -ForegroundColor Yellow
        return
    }

    Write-Host ""
    Write-Host "  VERSIONES DEL MODELO DE EDAD" -ForegroundColor Cyan
    Write-Host "  $('─' * 72)"
    Write-Host ("  {0,-5}  {1,-38}  {2,6}  {3,-5}  {4}" -f "ID","Fichero","MAE","Umbral","Descripcion")
    Write-Host "  $('─' * 72)"

    foreach ($v in $registry.versions) {
        $mae     = if ($null -ne $v.mae_val) { "$($v.mae_val)a" } else { "  — " }
        $thr     = if ($null -ne $v.minor_threshold) { "<$($v.minor_threshold)" } else { "—" }
        $marker  = if ($v.active) { " ◀ ACTIVO" } else { "" }
        $color   = if ($v.active) { "Green" } else { "White" }
        Write-Host ("  {0,-5}  {1,-38}  {2,6}  {3,-5}  {4}{5}" -f `
            $v.id, $v.filename, $mae, $thr, $v.description, $marker) -ForegroundColor $color
    }
    Write-Host "  $('─' * 72)"
    Write-Host ""
}


# ── Comando: info ─────────────────────────────────────────────────────────────

function Invoke-Info($id) {
    if (-not $id) { Write-Host "Uso: manage_models.ps1 info <id>" -ForegroundColor Red; exit 1 }
    $registry = Get-Registry
    $v = Find-Version $registry $id
    if (-not $v) { Write-Host "Version '$id' no encontrada." -ForegroundColor Red; exit 1 }

    Write-Host ""
    Write-Host "  Detalles de $($v.id)" -ForegroundColor Cyan
    Write-Host "  Fichero    : $($v.filename)"
    Write-Host "  Fecha      : $($v.date)"
    Write-Host "  Descripcion: $($v.description)"
    Write-Host "  MAE val    : $(if ($null -ne $v.mae_val) { "$($v.mae_val) anos" } else { 'no registrado' })"
    Write-Host "  Alpha BCE  : $(if ($null -ne $v.alpha) { $v.alpha } else { '—' })"
    Write-Host "  Umbral     : $(if ($null -ne $v.minor_threshold) { "age < $($v.minor_threshold)" } else { '—' })"
    Write-Host "  Epocas     : $(if ($null -ne $v.epochs) { $v.epochs } else { '—' })"
    Write-Host "  Activo     : $($v.active)"
    Write-Host ""
}


# ── Comando: use ──────────────────────────────────────────────────────────────

function Invoke-Use($id) {
    if (-not $id) { Write-Host "Uso: manage_models.ps1 use <id>" -ForegroundColor Red; exit 1 }

    $registry = Get-Registry
    $v = Find-Version $registry $id
    if (-not $v) { Write-Host "Version '$id' no encontrada. Ejecuta 'list' para ver las disponibles." -ForegroundColor Red; exit 1 }

    $src = Join-Path $ModelsDir $v.filename
    if (-not (Test-Path $src)) {
        Write-Host "Fichero '$src' no encontrado en disco." -ForegroundColor Red; exit 1
    }

    Write-Host ""
    Write-Host "==> Activando $id ($($v.filename))..." -ForegroundColor Cyan
    Copy-Item $src $ActivePath -Force
    Write-Host "    age_model.pth actualizado."

    # Marcar activo en registry
    foreach ($ver in $registry.versions) {
        $ver.active = ($ver.id -eq $id)
    }
    Save-Registry $registry
    Write-Host "    registry.json actualizado."

    # Reiniciar contenedores
    Write-Host ""
    Write-Host "==> Reiniciando contenedores..." -ForegroundColor Cyan
    Set-Location $ProjectRoot

    # age-realtime usa volumen: solo restart
    docker compose restart age-realtime
    if ($LASTEXITCODE -ne 0) { Write-Host "Aviso: no se pudo reiniciar age-realtime." -ForegroundColor Yellow }
    else { Write-Host "    age-realtime reiniciado (volumen, sin rebuild)." }

    # age-detection tiene el modelo copiado en la imagen: rebuild necesario
    Write-Host ""
    Write-Host "    age-detection requiere rebuild para incluir el nuevo modelo." -ForegroundColor Yellow
    $answer = Read-Host "    Reconstruir age-detection ahora? [S/n]"
    if ($answer -eq "" -or $answer -match "^[sS]") {
        docker compose up -d --build age-detection
        if ($LASTEXITCODE -ne 0) { Write-Host "Error en rebuild de age-detection." -ForegroundColor Red; exit 1 }
        Write-Host "    age-detection reconstruido y arrancado."
    } else {
        Write-Host "    Rebuild omitido. Ejecuta manualmente: docker compose up -d --build age-detection" -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Host "Modelo $id activo." -ForegroundColor Green
    Write-Host ""
}


# ── Despacho ──────────────────────────────────────────────────────────────────

switch ($Command.ToLower()) {
    "list" { Invoke-List }
    "info" { Invoke-Info $Version }
    "use"  { Invoke-Use  $Version }
    default {
        Write-Host "Comando desconocido: '$Command'. Usa: list | use <id> | info <id>" -ForegroundColor Red
        exit 1
    }
}
