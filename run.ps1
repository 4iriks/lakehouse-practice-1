param([ValidateSet('Setup','Load','Trino','Spark','Metadata','Evolution','Report','All')][string]$Stage='All')
$ErrorActionPreference='Stop'
Set-Location $PSScriptRoot
function Checked([scriptblock]$Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code $LASTEXITCODE" }
}
if ($Stage -in @('Setup','All')) {
    if (-not (Test-Path '.venv/Scripts/python.exe')) { Checked { python -m venv .venv } }
    Checked { .\.venv\Scripts\python.exe -m pip install -r requirements-lock.txt }
    Checked { .\.venv\Scripts\python.exe scripts/prepare_source.py }
    Checked { docker compose build minio }
    Checked { docker compose up -d --wait postgres minio lakekeeper }
    Checked { .\.venv\Scripts\python.exe scripts/lab.py setup }
    Checked { docker compose up -d --wait trino }
}
if ($Stage -in @('Load','All')) { Checked { .\.venv\Scripts\python.exe scripts/lab.py load } }
if ($Stage -in @('Trino','All')) { Checked { .\.venv\Scripts\python.exe scripts/lab.py benchmark } }
if ($Stage -in @('Metadata','All')) { Checked { .\.venv\Scripts\python.exe scripts/lab.py metadata } }
if ($Stage -in @('Evolution','All')) { Checked { .\.venv\Scripts\python.exe scripts/lab.py evolution } }
if ($Stage -in @('Spark','All')) {
    Checked { docker compose stop trino }
    $variants = @('parquet_gzip','parquet_zstd','orc_snappy','orc_zstd')
    foreach ($series in 1..3) {
        foreach ($offset in 0..3) {
            $variant = $variants[($series - 1 + $offset) % 4]
            Checked { docker compose --profile spark run --rm spark /lab/scripts/spark_benchmark.py --variant $variant --series $series }
        }
    }
    Checked { docker compose up -d --wait trino }
}
if ($Stage -in @('Report','All')) {
    Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors |
        ConvertTo-Json | Set-Content -Encoding UTF8 results/hardware_cpu.json
    Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory |
        ConvertTo-Json | Set-Content -Encoding UTF8 results/hardware_system.json
    $dockerInfo = docker info --format '{{json .}}' | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect Docker' }
    $dockerInfo | Select-Object ServerVersion,OSType,Architecture,NCPU,MemTotal,OperatingSystem,KernelVersion |
        ConvertTo-Json | Set-Content -Encoding UTF8 results/docker_environment.json
    Checked { .\.venv\Scripts\python.exe scripts/verify.py }
    Checked { .\.venv\Scripts\python.exe scripts/make_report.py }
}
