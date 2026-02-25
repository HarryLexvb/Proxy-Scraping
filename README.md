# OSIPTEL Scraper v1.0

Sistema de scraping automático de líneas telefónicas de OSIPTEL por RUC con **auto-detección de recursos del sistema** y **configuración optimizada**.

---

## 🚀 Inicio Rápido

### Instalación Automática Completa

**¡NUEVO!** El instalador ahora incluye **instalación automática de Python** si no lo tienes instalado.

**TODO EN UNO**: Ejecuta el instalador que prepara el entorno completo:

```batch
start.bat
```

El instalador automáticamente:
0. ✅ **Verifica e instala Python 3.12** (si no está instalado)
1. ✅ **Verifica la versión de Python** (mínimo 3.10+)
2. ✅ **Crea el entorno virtual** (`venv/`)
3. ✅ **Activa el entorno virtual** 
4. ✅ **Instala todas las dependencias** (camoufox, pandas, aiohttp, psutil, etc.)
5. ✅ **Descarga el browser Camoufox**
6. ✅ **Crea la estructura de carpetas**
7. ✅ **Deja el entorno ACTIVADO** para que ejecutes inmediatamente

**El script es inteligente**:
- Si **NO tienes Python**: Lo descarga e instala automáticamente (Python 3.12.10)
- Si **YA tienes Python**: Verifica que sea versión 3.10+ y continúa con la instalación
- Si tienes una **versión antigua**: Te avisa y te indica cómo actualizarlo

**Después de ejecutar `start.bat`**, en la MISMA ventana ejecuta:

```batch
python run_auto.py rucs/tu_archivo.csv
```

O con límite de bandwidth personalizado (en MB):

```batch
python run_auto.py rucs/tu_archivo.csv --bandwidth 5000
```

---

### Para Ejecuciones Futuras

Si cierras la terminal y quieres ejecutar el scraper de nuevo:

```batch
venv\Scripts\activate
python run_auto.py rucs/tu_archivo.csv
```

**Nota**: Solo necesitas ejecutar `start.bat` **UNA VEZ** (primera instalación). Después solo activas el entorno con `venv\Scripts\activate`.

---

## 📋 Requisitos

- **Windows 10/11** (64-bit)
- **4GB RAM mínimo** (recomendado 8GB+)
- **Conexión a internet estable**
- **Python 3.10+** - ¡Ahora se instala automáticamente si no lo tienes!

### Notas sobre Python:
- El instalador detecta automáticamente si tienes Python instalado
- Si no lo tienes, descarga e instala Python 3.12.10 automáticamente
- Si ya tienes Python, verifica que sea versión 3.10 o superior
- Versiones recomendadas: Python 3.10, 3.11 o 3.12

---

## 🎯 Modos de Ejecución

Al ejecutar `run_auto.py`, se mostrará un menú:

```
╔══════════════════════════════════════════════════════════════════════════════╗
║       OSIPTEL SCRAPER v1.0 - MODO DE EJECUCIÓN                               ║
╚══════════════════════════════════════════════════════════════════════════════╝

Selecciona el modo de configuración de workers:

  [1] Automático - Analizar PC y asignar workers automáticamente
  [2] Manual     - Especificar cantidad de workers manualmente
```

### Modo Automático (Recomendado)

El sistema analiza automáticamente:
- **CPU**: Detecta núcleos físicos disponibles
- **RAM**: Calcula memoria disponible (2GB por worker)
- **Red**: Mide latencia al proxy y ajusta delays

Ejemplo de detección:

```
⚡ RECURSOS DEL SISTEMA DETECTADOS:
   CPU:               8 cores (físicos)
   RAM disponible:    5.9 GB
   Latencia proxy:    270 ms
   
⚡ CONFIGURACIÓN ÓPTIMA:
   Workers paralelos: 12
   Delay min-max:     1.0s - 2.0s
   Páginas/browser:   150
```

### Modo Manual

Te permite especificar la cantidad exacta de workers (1-50).

Útil si:
- Quieres forzar menos workers para ahorrar recursos
- Conoces la capacidad óptima de tu PC
- Necesitas probar diferentes configuraciones

---

## 📁 Formato del Archivo de RUCs

El archivo CSV debe tener los RUCs en la **primera columna** con header "RUC":

```csv
RUC
20100047218
20100130204
20293847560
```

- Los RUCs deben tener exactamente **11 dígitos**
- Formato: CSV (separado por comas)

---

## 📤 Archivos de Salida

Todos los resultados se guardan en `osiptel_output/`:

| Archivo | Descripción |
|---------|-------------|
| `resultados.csv` | Datos extraídos (RUC + líneas telefónicas) |
| `save/*.csv` | Guardados parciales automáticos |
| `performance_report.json` | Reporte detallado de rendimiento |
| `scraper.log` | Log completo de ejecución |

### Formato de Resultados

```csv
RUC,Modalidad_1,Numero_Telefonico_1,Empresa_Operadora_1,Modalidad_2,...
20100047218,MOVIL,987654321,MOVISTAR,FIJO,014567890,CLARO
20100130204,MOVIL,912345678,ENTEL,,,,
20293847560,,,,,,,
```

Si un RUC no tiene líneas registradas, aparece con campos vacíos.

---

## ⚙️ Configuración del Proxy

Edita `config.py` con tus credenciales de SmartProxy:

```python
# Credenciales SmartProxy
PROXY_HOST = "proxy.smartproxy.net"
PROXY_PORT = 3120
PROXY_USERNAME = "smart-haroldvpn_area-PE"  # Tu usuario con _area-PE
PROXY_PASSWORD = "tu_contraseña"

# Límites
MAX_BANDWIDTH_MB = 99000  # 99GB (ajusta según tu plan)
MAX_WORKERS = 12  # Fallback si la detección automática falla
```

**IMPORTANTE**: El formato del username es crítico:
- Debe incluir `_area-PE` al final para obtener IPs de Perú
- Ejemplo: `smart-haroldvpn_area-PE`

---

## 🔧 Opciones Avanzadas

### Ajustar Límite de Bandwidth

Por defecto el scraper intenta usar hasta 10GB por ejecución:

```batch
# Usar solo 5GB
python run_auto.py rucs/archivo.csv --bandwidth 5000

# Usar 20GB
python run_auto.py rucs/archivo.csv --bandwidth 20000
```

### Cambiar Directorio de Salida

```batch
python run_auto.py rucs/archivo.csv --output mis_resultados/
```

### Probar Conexión al Proxy

Antes de ejecutar un scraping masivo, verifica la conexión:

```batch
python test_proxy.py
```

---

## 🔄 Reanudación Automática

Si el scraping se interrumpe (Ctrl+C, error, límite de bandwidth), el sistema:

1. **Guarda todo el progreso** automáticamente
2. **Elimina los RUCs procesados** del archivo CSV original
3. **Crea un backup** del archivo original en `rucs/backups/`

Para continuar, simplemente ejecuta de nuevo:

```batch
python run_auto.py rucs/archivo.csv
```

El scraper detectará que hay RUCs pendientes y continuará desde donde quedó.

---

## 📊 Rendimiento Esperado

Con configuración automática óptima (12 workers):

| RUCs | Tiempo estimado |
|------|-----------------|
| 1,000 | ~1.5 horas |
| 5,000 | ~8 horas |
| 10,000 | ~16 horas |
| 20,000 | ~33 horas |

**Nota**: El tiempo varía según:
- Recursos del sistema (CPU, RAM)
- Velocidad de conexión a internet
- Latencia al proxy
- Cantidad de líneas por RUC

---

## 🛡️ Características de Seguridad

### Anti-Detección
- **Camoufox**: Browser anti-detección basado en Firefox
- **Rotación de sesiones**: Cada worker usa sesión única del proxy
- **Delays aleatorios**: Entre requests para simular comportamiento humano
- **Fingerprint humanizado**: User agents y configuraciones realistas

### Gestión de Errores
- **Reintentos automáticos**: Hasta 2 intentos por RUC
- **Detección de bandwidth**: Se detiene automáticamente al alcanzar límite
- **Guardado periódico**: Progreso guardado cada 25 RUCs
- **Manejo de interrupciones**: Ctrl+C guarda estado actual

---

## 📁 Estructura del Proyecto

```
osiptel-scraper-v1/
├── run_auto.py           # 🚀 Script principal de ejecución
├── system_optimizer.py   # ⚡ Auto-detección de recursos
├── osiptel_main.py       # 🎯 Orquestador de workers
├── osiptel_worker.py     # 👷 Lógica de scraping
├── osiptel_core.py       # 📦 Clases base y utilidades
├── config.py             # ⚙️ Configuración (EDITAR AQUÍ)
├── start.bat             # 🔧 Instalador de dependencias
├── requirements.txt      # 📋 Lista de dependencias
├── .gitignore            # 🚫 Archivos ignorados por Git
├── venv/                 # 🐍 Entorno virtual (creado por start.bat)
├── rucs/                 # 📂 Carpeta para archivos CSV (CREAR MANUALMENTE)
│   ├── tu_archivo.csv    # Tu archivo con RUCs a scrapear
│   └── backups/          # 💾 Backups automáticos (creado automáticamente)
└── osiptel_output/       # 📤 Resultados del scraping (creado automáticamente)
    ├── resultados.csv    # Datos extraídos
    ├── save/             # Guardados parciales
    ├── performance_report.json
    └── scraper.log
```

### 📋 Carpetas que debes crear manualmente:

**IMPORTANTE**: Antes de ejecutar el scraper, crea la carpeta `rucs/`:

```batch
mkdir rucs
```

Luego coloca tu archivo CSV con los RUCs dentro de `rucs/`.

### 🤖 Carpetas creadas automáticamente:

- ✅ `venv/` - Creada por `start.bat`
- ✅ `osiptel_output/` - Creada al ejecutar el scraper
- ✅ `rucs/backups/` - Creada al procesar RUCs

---

## 🔧 Solución de Problemas

### "Failed to create browser"

**Causa**: Error en la instalación de Camoufox.

**Solución**:
```batch
python -m camoufox fetch
```

### "Proxy connection failed"

**Causa**: Credenciales incorrectas o proxy no disponible.

**Solución**:
1. Verifica `config.py` con tus credenciales correctas
2. Ejecuta `python test_proxy.py` para validar
3. Verifica tu plan en dashboard.smartproxy.com

### "NotImplementedError in subprocess_exec"

**Causa**: Política de event loop incorrecta (ya corregido en v1.0).

**Solución**: Usa la versión actual de `run_auto.py` que no establece `WindowsSelectorEventLoopPolicy`.

### Muchos RUCs fallan

**Solución**:
1. Usa modo manual con menos workers (ej: 8)
2. Verifica que los RUCs tengan 11 dígitos
3. Revisa el log en `osiptel_output/scraper.log`

### Scraper muy lento

**Solución**:
1. Verifica tu conexión a internet
2. Prueba con modo manual y más workers
3. Reduce el valor de `MAX_RETRIES` en `osiptel_core.py`

---

## 📈 Estimación de Bandwidth

Cada RUC consume aproximadamente **0.5-1 MB** en promedio (con `BLOCK_IMAGES=True`).

| Plan | Bandwidth | RUCs aproximados |
|------|-----------|------------------|
| 10 GB | 10,000 MB | ~15,000 RUCs |
| 25 GB | 25,000 MB | ~40,000 RUCs |
| 50 GB | 50,000 MB | ~80,000 RUCs |
| 100 GB | 100,000 MB | ~160,000 RUCs |

**Recomendación**: Siempre deja un buffer de 5-10% para evitar cortes abruptos.

---

## 🛠️ Dependencias Instaladas

El archivo `requirements.txt` incluye:

```
camoufox>=0.4.11      # Browser anti-detección
aiofiles>=24.0.0      # Operaciones de archivos async
aiohttp>=3.10.0       # Cliente HTTP async
pandas>=2.2.0         # Procesamiento de datos
openpyxl>=3.1.0       # Lectura/escritura Excel
psutil>=6.0.0         # Monitoreo del sistema
```

---

## 📞 Obtener Credenciales SmartProxy

1. Registrate en [smartproxy.com](https://smartproxy.com)
2. Compra un plan de Residential Proxies
3. Ve a Dashboard → Residential Proxies → Proxy Setup
4. Copia tus credenciales:
   - **Username**: Formato `smart-usuario_area-PE`
   - **Password**: Tu contraseña
   - **Host**: `proxy.smartproxy.net`
   - **Port**: `3120` (residential)

---

## 📝 Changelog

### v1.0 (2026-01-17)
- ✅ Auto-detección de recursos del sistema
- ✅ Menú interactivo (modo automático vs manual)
- ✅ Instalador simplificado (solo Windows)
- ✅ Eliminación automática de RUCs procesados
- ✅ Reportes JSON detallados
- ✅ Filtro de 100 registros por página implementado
- ✅ Fix: Event loop policy corregido para Windows

---

*OSIPTEL Scraper v1.0 - Optimizado para Windows*
