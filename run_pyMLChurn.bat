@echo off
setlocal

REM Ensure we are in the repo root (this script's directory)
pushd "%~dp0"

REM Ensure .env exists; if not, copy from .env.example
if not exist ".env" (
  if exist ".env.example" (
    echo [setup] Creating .env from .env.example
    copy /Y ".env.example" ".env" >nul
  )
)

REM Prefer .venv_build if present, else .venv; create .venv if neither exists
set "VENV_DIR=.venv_build"
if not exist "%VENV_DIR%\Scripts\python.exe" set "VENV_DIR=.venv"
if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [setup] Creating virtual environment in .venv
  py -3 -m venv .venv 2>nul || python -m venv .venv
  set "VENV_DIR=.venv"
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo [error] Failed to locate or create virtual environment.
  goto :end
)

echo [setup] Upgrading pip...
"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :end

echo [setup] Installing requirements...
"%VENV_DIR%\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto :end

echo [run] pyMLChurn.py %*
"%VENV_DIR%\Scripts\python.exe" pyMLChurn.py %*

:end
set EXITCODE=%ERRORLEVEL%
echo.
echo Exit code: %EXITCODE%
echo Press any key to close...
pause >nul
popd
exit /b %EXITCODE%
