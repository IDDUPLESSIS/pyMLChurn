Param(
  [int]$top = 1000,
  [string]$output,
  [ValidateSet('windows','sql')][string]$auth,
  [string]$username,
  [string]$password,
  [string]$driver,
  [switch]$NoEncrypt,
  [switch]$NoTrustCert
)

$ErrorActionPreference = 'Stop'

function Ensure-Venv {
  param([string]$Root)
  $venvBuild = Join-Path $Root '.venv_build/ScriptS/python.exe'
  $venv = Join-Path $Root '.venv/ScriptS/python.exe'

  if (Test-Path $venvBuild) { return (Join-Path $Root '.venv_build') }
  if (Test-Path $venv) { return (Join-Path $Root '.venv') }

  Write-Host '[setup] Creating virtual environment in .venv'
  if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3 -m venv (Join-Path $Root '.venv')
  } else {
    python -m venv (Join-Path $Root '.venv')
  }
  return (Join-Path $Root '.venv')
}

try {
  $root = Split-Path -Parent $MyInvocation.MyCommand.Path
  Set-Location $root

  $venvDir = Ensure-Venv -Root $root
  $py = Join-Path $venvDir 'Scripts/python.exe'

  if (!(Test-Path (Join-Path $root '.env')) -and (Test-Path (Join-Path $root '.env.example'))) {
    Copy-Item (Join-Path $root '.env.example') (Join-Path $root '.env') -Force
    Write-Host '[setup] Created .env from .env.example'
  }

  Write-Host '[setup] Upgrading pip...'
  & $py -m pip install --upgrade pip | Write-Host

  Write-Host '[setup] Installing requirements...'
  & $py -m pip install -r (Join-Path $root 'requirements.txt') | Write-Host

  $argsList = @()
  if ($top) { $argsList += @('--top', $top) }
  if ($output) { $argsList += @('--output', $output) }
  if ($auth) { $argsList += @('--auth', $auth) }
  if ($username) { $argsList += @('--username', $username) }
  if ($password) { $argsList += @('--password', $password) }
  if ($driver) { $argsList += @('--driver', $driver) }
  if ($NoEncrypt) { $argsList += '--no-encrypt' }
  if ($NoTrustCert) { $argsList += '--no-trust-cert' }

  Write-Host "[run] pyMLChurn.py $($argsList -join ' ')"
  & $py (Join-Path $root 'pyMLChurn.py') @argsList
}
catch {
  Write-Error $_
}
finally {
  Write-Host "`nDone."
}
