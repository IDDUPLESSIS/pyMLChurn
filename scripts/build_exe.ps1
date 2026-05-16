Param(
  [switch]$OneFile = $false,
  [string]$Name = 'pyMLChurn'
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootCandidate = Split-Path -Parent $scriptDir
if (Test-Path (Join-Path $rootCandidate 'pyMLChurn.py')) {
  $root = $rootCandidate
} else {
  $root = $scriptDir
}
Set-Location $root

Write-Host '[build] Ensuring venv at .venv_build'
if (-not (Test-Path .\.venv_build\Scripts\python.exe)) {
  python -m venv .venv_build
}

$py = '.\.venv_build\Scripts\python.exe'
Write-Host '[build] Installing PyInstaller'
& $py -m pip install --upgrade pip | Out-Null
& $py -m pip install pyinstaller | Out-Null

Write-Host '[build] Cleaning old dist/build'
if (Test-Path .\dist) { Remove-Item -Recurse -Force .\dist }
if (Test-Path .\build) { Remove-Item -Recurse -Force .\build }
if (Test-Path .\$Name.spec) { Remove-Item -Force .\$Name.spec }

$args = @('--name', $Name, 'pyMLChurn.py')
if ($OneFile) { $args = @('--onefile') + $args } else { $args = @('--onedir') + $args }

Write-Host "[build] Running PyInstaller $($args -join ' ')"
& $py -m PyInstaller --hidden-import importlib.resources --collect-submodules sklearn --collect-submodules scipy --collect-data scipy @args

Write-Host "[build] Done. Executable is under: dist\\$Name\\$Name.exe"



