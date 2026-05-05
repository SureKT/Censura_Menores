<#
.SYNOPSIS
    Informe de latencias end-to-end del pipeline.

.DESCRIPTION
    Consulta PostgreSQL y muestra, para cada solicitud completada,
    el tiempo total y el desglose por fase:
        Deteccion de caras | Estimacion de edad | Pixelado | Almacenamiento

    Los tiempos se expresan en milisegundos.

.PARAMETER Last
    Numero de solicitudes recientes a mostrar (default: 20).

.PARAMETER Summary
    Muestra solo el resumen agregado (media, min, max) sin el detalle por solicitud.

.EXAMPLE
    .\latency-report.ps1
    .\latency-report.ps1 -Last 50
    .\latency-report.ps1 -Summary
#>

param(
    [int]   $Last    = 20,
    [switch]$Summary
)

$ErrorActionPreference = "Stop"

# ── Consultas SQL ─────────────────────────────────────────────────────────────

$sqlDetail = "
SELECT
    GUID_Solicitud,
    Estado,
    ROUND(EXTRACT(EPOCH FROM (Fin_Solicitud                - Inicio_Solicitud               )) * 1000)::INT AS total_ms,
    ROUND(EXTRACT(EPOCH FROM (Fin_Deteccion_Caras          - Inicio_Deteccion_Caras         )) * 1000)::INT AS deteccion_ms,
    ROUND(EXTRACT(EPOCH FROM (Fin_edad                     - Inicio_Edad                    )) * 1000)::INT AS edad_ms,
    ROUND(EXTRACT(EPOCH FROM (Fin_Pixelado                 - Inicio_Pixelado                )) * 1000)::INT AS pixelado_ms,
    ROUND(EXTRACT(EPOCH FROM (Fin_Almacenamiento_Solicitud - Inicio_Almacenamiento_Solicitud)) * 1000)::INT AS almac_ms
FROM Solicitud
WHERE Estado = 'COMPLETADA'
ORDER BY Inicio_Solicitud DESC
LIMIT $Last;"

$sqlSummary = "SELECT COUNT(*), ROUND(AVG(EXTRACT(EPOCH FROM (Fin_Solicitud - Inicio_Solicitud)) * 1000))::INT, ROUND(MIN(EXTRACT(EPOCH FROM (Fin_Solicitud - Inicio_Solicitud)) * 1000))::INT, ROUND(MAX(EXTRACT(EPOCH FROM (Fin_Solicitud - Inicio_Solicitud)) * 1000))::INT, ROUND(AVG(EXTRACT(EPOCH FROM (Fin_Deteccion_Caras - Inicio_Deteccion_Caras)) * 1000))::INT, ROUND(AVG(EXTRACT(EPOCH FROM (Fin_edad - Inicio_Edad)) * 1000))::INT, ROUND(AVG(EXTRACT(EPOCH FROM (Fin_Pixelado - Inicio_Pixelado)) * 1000))::INT FROM Solicitud WHERE Estado = 'COMPLETADA';"

# ── Helper ────────────────────────────────────────────────────────────────────

function Invoke-Psql {
    param([string]$Sql)
    # Se pasa el SQL por stdin (-i) para evitar truncacion de argumentos largos en PowerShell
    $out = $Sql | docker exec -i postgres psql -U bda_user -d bda_imagenes -t -A -F "|"
    if ($LASTEXITCODE -ne 0) { throw "Error consultando PostgreSQL." }
    # @() fuerza array aunque solo haya una fila; evita que PowerShell trate
    # una cadena unica como array de caracteres al indexar con [0].
    return @($out | Where-Object { $_.Trim() -ne "" })
}

function Format-Ms {
    param([string]$Val)
    if ([string]::IsNullOrWhiteSpace($Val) -or $Val -eq "\N" -or $Val -eq "") {
        return "     n/a"
    }
    return ("$Val ms").PadLeft(8)
}

# ── Resumen agregado ──────────────────────────────────────────────────────────

Write-Host ""
Write-Host "  METRICAS DE LATENCIA DEL PIPELINE" -ForegroundColor Cyan
Write-Host ("  " + ("-" * 62))

$sumRows = @(Invoke-Psql -Sql $sqlSummary)
if (-not $sumRows) {
    Write-Host "  Sin solicitudes COMPLETADAS en la base de datos." -ForegroundColor Yellow
    exit 0
}

$s = ($sumRows[0]) -split [regex]::Escape("|")
if ($s[0] -eq "0") {
    Write-Host "  Sin solicitudes COMPLETADAS en la base de datos." -ForegroundColor Yellow
    exit 0
}

Write-Host ("  Solicitudes completadas : {0}"                              -f $s[0])
Write-Host ("  Latencia total          : avg {0} ms   min {1} ms   max {2} ms" -f $s[1], $s[2], $s[3])
Write-Host "  Fases (media):"
Write-Host ("    Deteccion de caras    : {0} ms"                           -f $s[4])
Write-Host ("    Estimacion de edad    : {0} ms"                           -f $s[5])
Write-Host ("    Pixelado              : {0} ms"                           -f $s[6])
Write-Host ("  " + ("-" * 62))

if ($Summary) { Write-Host ""; exit 0 }

# ── Detalle por solicitud ─────────────────────────────────────────────────────

Write-Host ""
Write-Host ("  {0,-36}  {1,9}  {2,9}  {3,8}  {4,8}" -f "GUID", "Total", "Deteccion", "Edad", "Pixelado")
Write-Host ("  " + ("-" * 78))

$rows = @(Invoke-Psql -Sql $sqlDetail)
foreach ($row in $rows) {
    $cols = $row -split [regex]::Escape("|")
    if ($cols.Count -lt 7) { continue }
    $guid    = $cols[0].Trim().Substring(0, [Math]::Min(36, $cols[0].Trim().Length))
    $total   = Format-Ms $cols[2].Trim()
    $det     = Format-Ms $cols[3].Trim()
    $edad    = Format-Ms $cols[4].Trim()
    $pix     = Format-Ms $cols[5].Trim()
    Write-Host ("  {0,-36}  {1}  {2}  {3}  {4}" -f $guid, $total, $det, $edad, $pix)
}

Write-Host ("  " + ("-" * 78))
Write-Host ""
Write-Host "  Nota: cada servicio es un consumer-group Kafka independiente." -ForegroundColor DarkGray
Write-Host "  Escala horizontalmente sin cambios de codigo." -ForegroundColor DarkGray
Write-Host ""
