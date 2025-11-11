@echo off
setlocal
REM Debug launcher that runs the EXE and captures all console output to a file

set EXE_DIR=%~dp0dist\pyMLChurn
set EXE=%EXE_DIR%\pyMLChurn.exe
set LOG=%EXE_DIR%\pyMLChurn_win_stdout_stderr.txt

if not exist "%EXE%" (
  echo [error] EXE not found at "%EXE%"
  echo Build the EXE first with: build_exe.ps1
  pause
  exit /b 1
)

REM Ensure .env is present next to the EXE for double-click behavior
if not exist "%EXE_DIR%\.env" (
  if exist "%~dp0.env" (
    copy /Y "%~dp0.env" "%EXE_DIR%\.env" >nul
    echo [info] Copied .env next to the EXE
  ) else (
    echo [warn] No .env found in repo root; EXE may not connect.
  )
)

echo [start] %date% %time% > "%LOG%"
pushd "%EXE_DIR%"
"%EXE%" 1>>"%LOG%" 2>&1
echo [exit ] %date% %time% ^(errorlevel=%errorlevel%^)>> "%LOG%"
popd
echo Wrote console log to: "%LOG%"
pause

