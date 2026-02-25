"""
================================================================================
OSIPTEL SCRAPER - SYSTEM OPTIMIZER v1.0
================================================================================
Detecta automáticamente los recursos del sistema y optimiza la configuración
para máxima velocidad de scraping.

Features:
- Auto-detección de CPU, RAM, conexión de red
- Cálculo óptimo de workers
- Detección de agotamiento de bandwidth del proxy
- Eliminación automática de RUCs procesados
- Reportes detallados de rendimiento
================================================================================
"""

import os
import sys
import platform
import psutil
import asyncio
import aiohttp
import time
import json
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('osiptel.optimizer')


@dataclass
class SystemResources:
    """Recursos del sistema detectados"""
    cpu_cores: int = 0
    cpu_threads: int = 0
    cpu_percent: float = 0.0
    ram_total_gb: float = 0.0
    ram_available_gb: float = 0.0
    ram_percent: float = 0.0
    os_name: str = ""
    python_version: str = ""
    
    # Network
    network_download_mbps: float = 0.0
    network_latency_ms: float = 0.0
    
    # Calculated
    optimal_workers: int = 0
    optimal_delay_min: float = 0.0
    optimal_delay_max: float = 0.0
    max_pages_per_browser: int = 0


@dataclass
class BandwidthTracker:
    """Rastrea el uso de bandwidth en tiempo real"""
    bytes_sent: int = 0
    bytes_received: int = 0
    start_time: float = field(default_factory=time.time)
    last_check_time: float = field(default_factory=time.time)
    
    # Límites
    max_bandwidth_mb: float = 10000  # 10GB default
    warning_threshold: float = 0.90  # 90%
    
    # Tracking por período
    bytes_last_minute: int = 0
    bytes_last_5_minutes: int = 0
    
    # Estado
    is_exhausted: bool = False
    exhaustion_reason: str = ""
    
    def get_used_mb(self) -> float:
        return (self.bytes_sent + self.bytes_received) / (1024 * 1024)
    
    def get_used_percent(self) -> float:
        return (self.get_used_mb() / self.max_bandwidth_mb) * 100
    
    def get_remaining_mb(self) -> float:
        return self.max_bandwidth_mb - self.get_used_mb()
    
    def is_near_limit(self) -> bool:
        return self.get_used_percent() >= (self.warning_threshold * 100)
    
    def update(self, bytes_sent: int, bytes_received: int):
        self.bytes_sent += bytes_sent
        self.bytes_received += bytes_received
        self.last_check_time = time.time()


@dataclass
class PerformanceReport:
    """Reporte de rendimiento del scraper"""
    # Tiempos
    start_time: datetime = None
    end_time: datetime = None
    total_duration_seconds: float = 0.0
    
    # RUCs
    total_rucs_to_process: int = 0
    rucs_processed: int = 0
    rucs_successful: int = 0
    rucs_failed: int = 0
    rucs_remaining: int = 0
    
    # Velocidad
    rucs_per_minute: float = 0.0
    rucs_per_hour: float = 0.0
    rucs_per_worker_per_hour: float = 0.0
    
    # Bandwidth
    bandwidth_used_mb: float = 0.0
    bandwidth_remaining_mb: float = 0.0
    avg_kb_per_ruc: float = 0.0
    
    # Workers
    workers_used: int = 0
    avg_worker_efficiency: float = 0.0
    
    # Errores
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    retry_count: int = 0
    
    # Razón de finalización
    finish_reason: str = ""  # "completed", "bandwidth_exhausted", "interrupted", "error"
    
    def calculate_metrics(self):
        """Calcula métricas derivadas"""
        if self.end_time and self.start_time:
            self.total_duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        if self.total_duration_seconds > 0:
            minutes = self.total_duration_seconds / 60
            hours = self.total_duration_seconds / 3600
            
            self.rucs_per_minute = self.rucs_processed / minutes if minutes > 0 else 0
            self.rucs_per_hour = self.rucs_processed / hours if hours > 0 else 0
            
            if self.workers_used > 0 and hours > 0:
                self.rucs_per_worker_per_hour = self.rucs_per_hour / self.workers_used
        
        if self.rucs_processed > 0 and self.bandwidth_used_mb > 0:
            self.avg_kb_per_ruc = (self.bandwidth_used_mb * 1024) / self.rucs_processed
        
        self.rucs_remaining = self.total_rucs_to_process - self.rucs_processed
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario para JSON"""
        self.calculate_metrics()
        return {
            "timing": {
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "end_time": self.end_time.isoformat() if self.end_time else None,
                "total_duration": str(timedelta(seconds=int(self.total_duration_seconds))),
                "total_seconds": round(self.total_duration_seconds, 2)
            },
            "rucs": {
                "total_to_process": self.total_rucs_to_process,
                "processed": self.rucs_processed,
                "successful": self.rucs_successful,
                "failed": self.rucs_failed,
                "remaining": self.rucs_remaining,
                "success_rate": round((self.rucs_successful / self.rucs_processed * 100) if self.rucs_processed > 0 else 0, 2)
            },
            "speed": {
                "rucs_per_minute": round(self.rucs_per_minute, 2),
                "rucs_per_hour": round(self.rucs_per_hour, 2),
                "rucs_per_worker_per_hour": round(self.rucs_per_worker_per_hour, 2)
            },
            "bandwidth": {
                "used_mb": round(self.bandwidth_used_mb, 2),
                "remaining_mb": round(self.bandwidth_remaining_mb, 2),
                "avg_kb_per_ruc": round(self.avg_kb_per_ruc, 2)
            },
            "workers": {
                "count": self.workers_used,
                "avg_efficiency": round(self.avg_worker_efficiency, 2)
            },
            "errors": self.errors_by_type,
            "retry_count": self.retry_count,
            "finish_reason": self.finish_reason
        }
    
    def print_report(self):
        """Imprime reporte formateado"""
        self.calculate_metrics()
        
        duration = timedelta(seconds=int(self.total_duration_seconds))
        success_rate = (self.rucs_successful / self.rucs_processed * 100) if self.rucs_processed > 0 else 0
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         REPORTE FINAL DE SCRAPING                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ⏱️  TIEMPO                                                                   ║
║     Inicio:              {self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time else 'N/A':<30}                 ║
║     Fin:                 {self.end_time.strftime('%Y-%m-%d %H:%M:%S') if self.end_time else 'N/A':<30}                 ║
║     Duración total:      {str(duration):<30}                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📊 RUCs                                                                     ║
║     Total a procesar:    {self.total_rucs_to_process:>10,}                                          ║
║     Procesados:          {self.rucs_processed:>10,}                                          ║
║     Exitosos:            {self.rucs_successful:>10,}                                          ║
║     Fallidos:            {self.rucs_failed:>10,}                                          ║
║     Restantes:           {self.rucs_remaining:>10,}                                          ║
║     Tasa de éxito:       {success_rate:>10.1f}%                                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🚀 VELOCIDAD                                                                ║
║     RUCs por minuto:     {self.rucs_per_minute:>10.1f}                                          ║
║     RUCs por hora:       {self.rucs_per_hour:>10.1f}                                          ║
║     Por worker/hora:     {self.rucs_per_worker_per_hour:>10.1f}                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📡 BANDWIDTH                                                                ║
║     Usado:               {self.bandwidth_used_mb:>10.2f} MB                                      ║
║     Restante:            {self.bandwidth_remaining_mb:>10.2f} MB                                      ║
║     Promedio por RUC:    {self.avg_kb_per_ruc:>10.2f} KB                                      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ⚙️  WORKERS                                                                  ║
║     Cantidad usada:      {self.workers_used:>10}                                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📋 FINALIZACIÓN                                                             ║
║     Razón:               {self.finish_reason:<40}            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
        print(report)
        
        if self.errors_by_type:
            print("\n  📛 ERRORES POR TIPO:")
            for error_type, count in sorted(self.errors_by_type.items(), key=lambda x: -x[1]):
                print(f"     {error_type}: {count}")


class SystemOptimizer:
    """
    Detecta recursos del sistema y calcula configuración óptima.
    """
    
    def __init__(self):
        self.resources = SystemResources()
        self.logger = logging.getLogger('osiptel.optimizer')
    
    def detect_system_resources(self) -> SystemResources:
        """Detecta todos los recursos del sistema"""
        
        # CPU
        self.resources.cpu_cores = psutil.cpu_count(logical=False) or 4
        self.resources.cpu_threads = psutil.cpu_count(logical=True) or 8
        self.resources.cpu_percent = psutil.cpu_percent(interval=1)
        
        # RAM
        ram = psutil.virtual_memory()
        self.resources.ram_total_gb = ram.total / (1024 ** 3)
        self.resources.ram_available_gb = ram.available / (1024 ** 3)
        self.resources.ram_percent = ram.percent
        
        # OS y Python
        self.resources.os_name = f"{platform.system()} {platform.release()}"
        self.resources.python_version = platform.python_version()
        
        return self.resources
    
    async def test_network_speed(self) -> Tuple[float, float]:
        """Prueba velocidad de red y latencia"""
        try:
            # Test de latencia a múltiples endpoints
            test_urls = [
                'https://www.google.com',
                'https://www.cloudflare.com',
                'https://api64.ipify.org'
            ]
            
            latencies = []
            async with aiohttp.ClientSession() as session:
                for url in test_urls:
                    try:
                        start = time.time()
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                            await resp.text()
                        latency = (time.time() - start) * 1000
                        latencies.append(latency)
                    except:
                        continue
            
            # Usar la mejor latencia (más representativa de la conexión)
            if latencies:
                latency = min(latencies)
            else:
                latency = 300  # Default si no se pudo medir
            
            self.resources.network_latency_ms = latency
            
            # Estimación de velocidad basada en latencia
            if latency < 100:
                self.resources.network_download_mbps = 100
            elif latency < 200:
                self.resources.network_download_mbps = 80
            elif latency < 300:
                self.resources.network_download_mbps = 50
            elif latency < 500:
                self.resources.network_download_mbps = 30
            else:
                self.resources.network_download_mbps = 20
                
            return self.resources.network_download_mbps, latency
            
        except Exception as e:
            self.logger.warning(f"No se pudo probar la red: {e}")
            self.resources.network_download_mbps = 30  # Valor conservador
            self.resources.network_latency_ms = 300
            return 30, 300
    
    def calculate_optimal_workers(self) -> int:
        """
        Calcula el número óptimo de workers basado en:
        - Núcleos de CPU
        - RAM disponible
        - Velocidad de red
        - Latencia del proxy
        
        Cada browser Camoufox usa ~200-400MB de RAM
        """
        
        # Factor CPU: 2-3 workers por núcleo físico
        cpu_factor = self.resources.cpu_cores * 2.5
        
        # Factor RAM: cada browser usa ~300MB
        ram_mb = self.resources.ram_available_gb * 1024
        ram_factor = ram_mb / 350  # 350MB por browser (con margen)
        
        # Factor red: más workers si buena conexión
        if self.resources.network_latency_ms < 100:
            network_factor = 20  # Excelente conexión
        elif self.resources.network_latency_ms < 200:
            network_factor = 16
        elif self.resources.network_latency_ms < 400:
            network_factor = 12
        else:
            network_factor = 8
        
        # El mínimo de los tres factores
        optimal = int(min(cpu_factor, ram_factor, network_factor))
        
        # Límites
        optimal = max(4, min(optimal, 24))  # Entre 4 y 24 workers
        
        self.resources.optimal_workers = optimal
        return optimal
    
    def calculate_optimal_delays(self) -> Tuple[float, float]:
        """
        Calcula delays óptimos para máxima velocidad sin ser baneado.
        Con buena conexión podemos ser más agresivos.
        """
        
        # Delay mínimo basado en latencia
        if self.resources.network_latency_ms < 100:
            min_delay = 0.5
            max_delay = 1.0
        elif self.resources.network_latency_ms < 200:
            min_delay = 0.8
            max_delay = 1.5
        elif self.resources.network_latency_ms < 400:
            min_delay = 1.0
            max_delay = 2.0
        else:
            min_delay = 1.5
            max_delay = 3.0
        
        self.resources.optimal_delay_min = min_delay
        self.resources.optimal_delay_max = max_delay
        
        return min_delay, max_delay
    
    def calculate_pages_per_browser(self) -> int:
        """
        Calcula páginas por browser antes de reiniciar.
        Más RAM = más páginas posibles.
        """
        if self.resources.ram_available_gb > 8:
            pages = 200
        elif self.resources.ram_available_gb > 4:
            pages = 150
        elif self.resources.ram_available_gb > 2:
            pages = 100
        else:
            pages = 50
        
        self.resources.max_pages_per_browser = pages
        return pages
    
    async def optimize(self) -> SystemResources:
        """Ejecuta todas las optimizaciones"""
        
        print("\n🔍 Detectando recursos del sistema...")
        self.detect_system_resources()
        
        print("   📊 CPU:")
        print(f"      Núcleos físicos: {self.resources.cpu_cores}")
        print(f"      Hilos lógicos: {self.resources.cpu_threads}")
        print(f"      Uso actual: {self.resources.cpu_percent}%")
        
        print("   💾 RAM:")
        print(f"      Total: {self.resources.ram_total_gb:.1f} GB")
        print(f"      Disponible: {self.resources.ram_available_gb:.1f} GB")
        print(f"      Uso actual: {self.resources.ram_percent}%")
        
        print("\n🌐 Probando conexión de red...")
        speed, latency = await self.test_network_speed()
        print(f"   Latencia al proxy: {latency:.0f} ms")
        
        print("\n⚙️  Calculando configuración óptima...")
        workers = self.calculate_optimal_workers()
        min_delay, max_delay = self.calculate_optimal_delays()
        pages = self.calculate_pages_per_browser()
        
        print(f"   Workers óptimos: {workers}")
        print(f"   Delay entre requests: {min_delay}-{max_delay}s")
        print(f"   Páginas por browser: {pages}")
        
        return self.resources
    
    def print_summary(self):
        """Imprime resumen de la configuración detectada"""
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    CONFIGURACIÓN OPTIMIZADA DETECTADA                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  🖥️  SISTEMA: {self.resources.os_name:<48}            ║
║  🐍 PYTHON: {self.resources.python_version:<50}            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📊 CPU: {self.resources.cpu_cores} núcleos / {self.resources.cpu_threads} hilos ({self.resources.cpu_percent:.0f}% uso actual)
║  💾 RAM: {self.resources.ram_available_gb:.1f} GB disponible de {self.resources.ram_total_gb:.1f} GB
║  🌐 RED: {self.resources.network_latency_ms:.0f}ms latencia
╠══════════════════════════════════════════════════════════════════════════════╣
║  ⚡ CONFIGURACIÓN ÓPTIMA PARA MÁXIMA VELOCIDAD:                              ║
║     Workers paralelos:    {self.resources.optimal_workers:>3}                                              ║
║     Delay min-max:        {self.resources.optimal_delay_min:.1f}s - {self.resources.optimal_delay_max:.1f}s                                      ║
║     Páginas/browser:      {self.resources.max_pages_per_browser:>3}                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")


class BandwidthMonitor:
    """
    Monitorea el uso de bandwidth y detecta agotamiento.
    """
    
    def __init__(self, max_bandwidth_mb: float = 10000):
        self.tracker = BandwidthTracker(max_bandwidth_mb=max_bandwidth_mb)
        self.initial_net_io = psutil.net_io_counters()
        self.last_net_io = self.initial_net_io
        self.estimated_per_ruc_kb = 500  # Estimación inicial
        self.actual_samples = []
        self.logger = logging.getLogger('osiptel.bandwidth')
        
        # Detección de errores de proxy
        self.consecutive_proxy_errors = 0
        self.max_consecutive_errors = 10
    
    def update_from_system(self):
        """Actualiza el tracking desde contadores del sistema"""
        current = psutil.net_io_counters()
        
        bytes_sent = current.bytes_sent - self.initial_net_io.bytes_sent
        bytes_recv = current.bytes_recv - self.initial_net_io.bytes_recv
        
        self.tracker.bytes_sent = bytes_sent
        self.tracker.bytes_received = bytes_recv
        self.tracker.last_check_time = time.time()
    
    def record_ruc_bandwidth(self, bytes_used: int):
        """Registra el bandwidth usado por un RUC"""
        kb_used = bytes_used / 1024
        self.actual_samples.append(kb_used)
        
        # Actualizar estimación con promedio móvil
        if len(self.actual_samples) > 10:
            self.estimated_per_ruc_kb = sum(self.actual_samples[-100:]) / len(self.actual_samples[-100:])
    
    def record_proxy_error(self):
        """Registra un error de proxy"""
        self.consecutive_proxy_errors += 1
    
    def record_success(self):
        """Registra un éxito"""
        self.consecutive_proxy_errors = 0
    
    def check_exhaustion(self) -> Tuple[bool, str]:
        """
        Verifica si el bandwidth está agotado.
        Returns: (is_exhausted, reason)
        """
        self.update_from_system()
        
        # 1. Verificar por porcentaje usado
        used_percent = self.tracker.get_used_percent()
        if used_percent >= 95:
            return True, f"Bandwidth usado: {used_percent:.1f}% (límite: 95%)"
        
        # 2. Verificar por errores consecutivos de proxy
        if self.consecutive_proxy_errors >= self.max_consecutive_errors:
            return True, f"Demasiados errores de proxy consecutivos: {self.consecutive_proxy_errors}"
        
        # 3. Advertir si cerca del límite
        if used_percent >= 85:
            self.logger.warning(f"⚠️ Bandwidth al {used_percent:.1f}% - Cerca del límite")
        
        return False, ""
    
    def estimate_remaining_rucs(self) -> int:
        """Estima cuántos RUCs más se pueden procesar"""
        remaining_kb = self.tracker.get_remaining_mb() * 1024
        return int(remaining_kb / self.estimated_per_ruc_kb)
    
    def get_status(self) -> Dict[str, Any]:
        """Obtiene estado actual del bandwidth"""
        self.update_from_system()
        return {
            "used_mb": round(self.tracker.get_used_mb(), 2),
            "remaining_mb": round(self.tracker.get_remaining_mb(), 2),
            "used_percent": round(self.tracker.get_used_percent(), 2),
            "estimated_rucs_remaining": self.estimate_remaining_rucs(),
            "avg_kb_per_ruc": round(self.estimated_per_ruc_kb, 2)
        }


class RUCFileManager:
    """
    Gestiona el archivo de RUCs: elimina procesados, mantiene pendientes.
    """
    
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.backup_file = input_file + ".backup"
        self.logger = logging.getLogger('osiptel.filemanager')
    
    def create_backup(self):
        """Crea backup del archivo original"""
        import shutil
        if os.path.exists(self.input_file) and not os.path.exists(self.backup_file):
            shutil.copy2(self.input_file, self.backup_file)
            self.logger.info(f"Backup creado: {self.backup_file}")
    
    def remove_processed_rucs(self, processed_rucs: set) -> Tuple[int, int]:
        """
        Elimina los RUCs procesados del archivo original.
        Returns: (rucs_removed, rucs_remaining)
        """
        import pandas as pd
        
        try:
            # Leer archivo actual
            df = pd.read_csv(self.input_file, dtype=str)
            original_count = len(df)
            
            # Identificar columna de RUC
            ruc_column = None
            for col in df.columns:
                if 'ruc' in col.lower() or df[col].astype(str).str.match(r'^\d{11}$').any():
                    ruc_column = col
                    break
            
            if ruc_column is None:
                ruc_column = df.columns[0]
            
            # Filtrar RUCs no procesados
            df[ruc_column] = df[ruc_column].astype(str).str.strip()
            df_remaining = df[~df[ruc_column].isin(processed_rucs)]
            
            remaining_count = len(df_remaining)
            removed_count = original_count - remaining_count
            
            # Guardar archivo actualizado
            df_remaining.to_csv(self.input_file, index=False)
            
            self.logger.info(f"✓ Archivo actualizado: {removed_count} RUCs eliminados, {remaining_count} restantes")
            
            return removed_count, remaining_count
            
        except Exception as e:
            self.logger.error(f"Error actualizando archivo: {e}")
            return 0, 0
    
    def get_remaining_count(self) -> int:
        """Cuenta RUCs restantes en el archivo"""
        try:
            import pandas as pd
            df = pd.read_csv(self.input_file, dtype=str)
            return len(df)
        except:
            return 0
