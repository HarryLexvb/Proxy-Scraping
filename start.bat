@echo off
REM ============================================================================
REM OSIPTEL SCRAPER v1.0 - INSTALACIÓN COMPLETA CON AUTO-INSTALACIÓN DE PYTHON
REM ============================================================================
REM Este script:
REM   0. Verifica e instala Python si es necesario (3.12.x)
REM   1. Verifica la versión de Python (minimo 3.10)
REM   2. Crea el entorno virtual (si no existe)
REM   3. Actualiza pip
REM   4. Instala todas las dependencias
REM   5. Descarga Camoufox browser
REM   6. Crea carpeta rucs/ si no existe
REM
REM Uso:
REM   start.bat
REM ============================================================================

echo.
echo ============================================================================
echo          OSIPTEL SCRAPER v1.0 - INSTALACION COMPLETA
echo ============================================================================
echo.

REM ============================================================================
REM PASO 0: VERIFICAR E INSTALAR PYTHON SI ES NECESARIO
REM ============================================================================
echo [PASO 0/6] Verificando Python...

REM Verificar si Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [INFO] Python no detectado en el sistema
    echo [INFO] Iniciando instalacion automatica de Python 3.12...
    echo.
    
    REM Crear carpeta temporal para el instalador
    if not exist "%TEMP%\osiptel_install" mkdir "%TEMP%\osiptel_install"
    
    REM Descargar Python 3.12.10 (versión estable más reciente)
    echo [PASO 0.1/6] Descargando Python 3.12.10...
    echo              Esto puede tomar varios minutos...
    powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe' -OutFile '%TEMP%\osiptel_install\python_installer.exe'}"
    
    if errorlevel 1 (
        echo [ERROR] No se pudo descargar Python
        echo         Por favor, descarga manualmente desde https://www.python.org/
        pause
        exit /b 1
    )
    echo [OK] Python descargado correctamente
    
    REM Instalar Python silenciosamente con todas las opciones necesarias
    echo [PASO 0.2/6] Instalando Python 3.12.10...
    echo              Esto puede tomar varios minutos...
    "%TEMP%\osiptel_install\python_installer.exe" /quiet InstallAllUsers=1 PrependPath=1 Include_test=0 Include_doc=0 Include_launcher=1 SimpleInstall=1
    
    if errorlevel 1 (
        echo [ERROR] Error durante la instalacion de Python
        pause
        exit /b 1
    )
    
    REM Esperar a que la instalación se complete
    timeout /t 10 /nobreak >nul
    
    REM Limpiar archivos temporales
    del /q "%TEMP%\osiptel_install\python_installer.exe" 2>nul
    rmdir /q "%TEMP%\osiptel_install" 2>nul
    
    echo [OK] Python instalado correctamente
    echo [INFO] Actualizando variables de entorno...
    
    REM Refrescar variables de entorno (añadir Python al PATH de esta sesión)
    call refreshenv >nul 2>&1
    
    REM Si refreshenv no está disponible, intentar actualizar PATH manualmente
    if errorlevel 1 (
        for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "SysPATH=%%b"
        for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UsrPATH=%%b"
        set "PATH=%UsrPATH%;%SysPATH%"
    )
    
    echo [OK] Python agregado al PATH del sistema
    echo.
    echo [IMPORTANTE] Si Python sigue sin detectarse, cierra esta ventana
    echo              y ejecuta start.bat nuevamente
    echo.
)

REM Verificar que Python está disponible ahora
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python sigue sin detectarse
    echo         Por favor, cierra esta ventana y ejecuta start.bat nuevamente
    echo         O reinicia tu computadora para que se apliquen los cambios
    pause
    exit /b 1
)

REM Obtener y mostrar versión de Python
for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [OK] Python %PYTHON_VERSION% detectado

REM Verificar que la versión es 3.10 o superior
for /f "tokens=1,2 delims=." %%a in ("%PYTHON_VERSION%") do (
    set MAJOR=%%a
    set MINOR=%%b
)

if %MAJOR% LSS 3 (
    echo [ERROR] Python %PYTHON_VERSION% es muy antiguo
    echo         Se requiere Python 3.10 o superior
    pause
    exit /b 1
)

if %MAJOR% EQU 3 if %MINOR% LSS 10 (
    echo [ERROR] Python %PYTHON_VERSION% es muy antiguo
    echo         Se requiere Python 3.10 o superior
    pause
    exit /b 1
)

echo [OK] Version de Python compatible (%PYTHON_VERSION%)
echo.

REM ============================================================================
REM PASO 1: CREAR ENTORNO VIRTUAL
REM ============================================================================
if not exist "venv" (
    echo [PASO 1/6] Creando entorno virtual...
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] No se pudo crear el entorno virtual
        echo         Asegurate de tener Python 3.10+ instalado
        pause
        exit /b 1
    )
    echo [OK] Entorno virtual creado
) else (
    echo [PASO 1/6] Entorno virtual ya existe
)

REM ============================================================================
REM PASO 2: ACTIVAR ENTORNO VIRTUAL
REM ============================================================================
echo [PASO 2/6] Activando entorno virtual...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] No se pudo activar el entorno virtual
    pause
    exit /b 1
)
echo [OK] Entorno virtual activado

REM ============================================================================
REM PASO 3: ACTUALIZAR PIP
REM ============================================================================
echo [PASO 3/6] Actualizando pip...
python -m pip install --upgrade pip --quiet
if errorlevel 1 (
    echo [ADVERTENCIA] Error actualizando pip, continuando...
) else (
    echo [OK] pip actualizado
)

REM ============================================================================
REM PASO 4: INSTALAR DEPENDENCIAS
REM ============================================================================
echo [PASO 4/6] Instalando dependencias...
echo            Esto puede tomar varios minutos...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ERROR] Error instalando dependencias
    echo         Revisa tu conexion a internet e intenta nuevamente
    pause
    exit /b 1
)
echo [OK] Dependencias instaladas correctamente

REM ============================================================================
REM PASO 5: DESCARGAR CAMOUFOX BROWSER
REM ============================================================================
echo [PASO 5/6] Descargando Camoufox browser...
echo            Esto puede tomar varios minutos la primera vez...
python -m camoufox fetch
if errorlevel 1 (
    echo [ADVERTENCIA] Error descargando Camoufox
    echo               Puedes intentar ejecutar manualmente: python -m camoufox fetch
) else (
    echo [OK] Camoufox browser listo
)

REM ============================================================================
REM PASO 6: CREAR CARPETA RUCS
REM ============================================================================
if not exist "rucs" (
    echo [PASO 6/6] Creando carpeta rucs/...
    mkdir rucs
    echo [OK] Carpeta rucs/ creada
) else (
    echo [PASO 6/6] Carpeta rucs/ ya existe
)

echo.
echo ============================================================================
echo                    ✓ INSTALACION COMPLETADA CON EXITO
echo ============================================================================
echo.
echo [✓] Python %PYTHON_VERSION% instalado y configurado
echo [✓] Entorno virtual creado y activado
echo [✓] Todas las dependencias instaladas
echo [✓] Camoufox browser descargado
echo [✓] Estructura de carpetas lista
echo.
echo ============================================================================
echo                   PROYECTO LISTO PARA EJECUTAR
echo ============================================================================
echo.
echo El entorno virtual esta ACTIVO. Ahora puedes ejecutar:
echo.
echo   python run_auto.py rucs/tu_archivo.csv
echo.
echo Ejemplos:
echo   python run_auto.py rucs/RUCS_a_scrapear.csv
echo   python run_auto.py rucs/RUCS_a_scrapear.csv --bandwidth 5000
echo.
echo ============================================================================
echo Para futuras ejecuciones, solo ejecuta:
echo   venv\Scripts\activate
echo   python run_auto.py rucs/tu_archivo.csv
echo ============================================================================
echo.
pause
echo.
echo Si cierras esta ventana, para volver a activar el entorno usa:
echo   venv\Scripts\activate
echo.
