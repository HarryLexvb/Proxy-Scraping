"""
================================================================================
OSIPTEL SCRAPER - CONFIGURATION FILE
================================================================================
IMPORTANT: Edit this file to set your SmartProxy credentials before running.

For 100 GB plan:
- RECOMMENDED RUCs: ~180,000 (safe limit with buffer)
- MAXIMUM THEORETICAL: ~200,000 (no retries, optimal conditions)
- Consumo estimado: ~0.5 MB por RUC
================================================================================
"""

# ============================================================================
# SMARTPROXY CREDENTIALS - EDIT THESE VALUES
# ============================================================================

PROXY_USERNAME = "smart-haroldvpn_area-PE"
PROXY_PASSWORD = "LXxRWv414OBG6tVW"

# Your proxy already has Peru IPs configured (_area-PE in username)
USE_PERU_IPS = False  # Not needed, already in username

# Custom proxy server (different from default)
PROXY_HOST = "proxy.smartproxy.net"
PROXY_PORT = 3120

# ============================================================================
# SCRAPER SETTINGS - Adjust if needed
# ============================================================================

# Number of parallel workers (increased now that proxy format is fixed)
MAX_WORKERS = 12

# Maximum bandwidth to use (MB) - Configurado como ilimitado
MAX_BANDWIDTH_MB = 999999999  # Virtualmente ilimitado (~1 millón de GB)

# Delays entre requests (reducidos para más velocidad)
MIN_DELAY = 1.0
MAX_DELAY = 2.0

# Máximo de reintentos por RUC
MAX_RETRIES = 2

# ============================================================================
# BANDWIDTH OPTIMIZATION
# ============================================================================

# Block images to save ~40% bandwidth (HIGHLY RECOMMENDED)
BLOCK_IMAGES = True

# ============================================================================
# OUTPUT SETTINGS
# ============================================================================

OUTPUT_DIRECTORY = "osiptel_output"
