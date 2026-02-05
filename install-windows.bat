@echo off
REM Swiss Caselaw - Windows Installer
REM Double-click this file to install

echo ========================================
echo   Swiss Caselaw - Local Search
echo   Installing...
echo ========================================
echo.

REM Get the directory where this script is located
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM Check for Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is required but not installed.
    echo.
    echo Please install Python from: https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Found Python %PYTHON_VERSION%

REM Create virtual environment
echo.
echo Setting up environment...
cd local_app

if not exist ".venv" (
    python -m venv .venv
)

call .venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
pip install --quiet -e .

REM Download database
echo.
echo Downloading database (this may take a while on first run)...
python -m caselaw_local.cli update

REM Create launcher script
cd /d "%SCRIPT_DIR%"
(
echo @echo off
echo cd /d "%%~dp0local_app"
echo call .venv\Scripts\activate.bat
echo echo Starting Swiss Caselaw...
echo echo.
echo echo Server running at http://127.0.0.1:8787
echo echo Press Ctrl+C to stop.
echo echo.
echo start http://127.0.0.1:8787
echo python -m caselaw_local.cli serve
echo pause
) > "Swiss Caselaw.bat"

REM Create Desktop shortcut
set DESKTOP=%USERPROFILE%\Desktop
if exist "%DESKTOP%" (
    copy "Swiss Caselaw.bat" "%DESKTOP%\Swiss Caselaw.bat" >nul
    echo.
    echo Created shortcut on Desktop!
)

echo.
echo ========================================
echo   Installation complete!
echo ========================================
echo.
echo To start Swiss Caselaw:
echo   - Double-click 'Swiss Caselaw.bat' on your Desktop
echo.
echo Starting now...
echo.

REM Start the app
call "Swiss Caselaw.bat"
