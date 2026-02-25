#!/usr/bin/env python3
"""
================================================================================
OSIPTEL SCRAPER v1.0 - MODO 100% AUTOMÁTICO
================================================================================
Scraper que se auto-configura y ejecuta sin intervención del usuario.

Características:
- Auto-detecta recursos del sistema (CPU, RAM, red)
- Calcula workers óptimos automáticamente
- Inicia scraping sin pedir confirmación
- Detecta agotamiento de bandwidth y se detiene
- Elimina RUCs procesados del archivo original
- Genera reportes detallados

Uso:
    python run_auto.py <archivo_rucs.csv> [--bandwidth <MB>]
    
Ejemplos:
    python run_auto.py rucs/RUCS_a_scrapear.csv
    python run_auto.py rucs/RUCS_a_scrapear.csv --bandwidth 10000
================================================================================
"""

import asyncio
import argparse
import sys
import os
import signal
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

import pandas as pd

# Imports locales
from config import (
    PROXY_USERNAME, PROXY_PASSWORD, PROXY_HOST, PROXY_PORT,
    MAX_BANDWIDTH_MB, BLOCK_IMAGES, OUTPUT_DIRECTORY
)
from osiptel_core import (
    ScraperConfig, ProxyConfig, TaskStatus, ErrorType,
    RUCResult, Statistics, setup_logging
)
from osiptel_main import ScraperOrchestrator
from system_optimizer import (
    SystemOptimizer, SystemResources, BandwidthMonitor,
    RUCFileManager, PerformanceReport
)


def create_optimized_config(
    resources: SystemResources,
    max_bandwidth_mb: float,
    output_dir: str
) -> ScraperConfig:
    """Crea configuración optimizada basada en recursos del sistema"""
    
    proxy = ProxyConfig(
        host=PROXY_HOST,
        port=PROXY_PORT,
        username=PROXY_USERNAME,
        password=PROXY_PASSWORD
    )
    
    config = ScraperConfig()
    config.proxy = proxy
    config.max_workers = resources.optimal_workers
    config.min_delay_between_requests = resources.optimal_delay_min
    config.max_delay_between_requests = resources.optimal_delay_max
    config.max_bandwidth_mb = int(max_bandwidth_mb)
    config.block_images = BLOCK_IMAGES
    config.headless = True
    config.humanize = True
    config.output_dir = output_dir
    config.max_retries = 3  # 3 reintentos para reducir falsos negativos
    
    return config


class AutoScraper:
    """
    Scraper 100% automático que no requiere intervención del usuario.
    """
    
    def __init__(
        self,
        input_file: str,
        max_bandwidth_mb: float = 10000,
        output_dir: str = "osiptel_output",
        manual_workers: Optional[int] = None
    ):
        self.input_file = input_file
        self.max_bandwidth_mb = max_bandwidth_mb
        self.output_dir = output_dir
        self.manual_workers = manual_workers  # Si es None, se usa auto-detección
        
        # Componentes
        self.optimizer = SystemOptimizer()
        self.file_manager = RUCFileManager(input_file)
        self.report = PerformanceReport()
        
        # Estado
        self.resources: Optional[SystemResources] = None
        self.config: Optional[ScraperConfig] = None
        self.orchestrator: Optional[ScraperOrchestrator] = None
        
        # Tracking
        self.processed_rucs: Set[str] = set()
        
        # Control
        self._interrupted = False
        
        # Setup logging
        self.logger = logging.getLogger('osiptel.auto')
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Maneja señales de interrupción"""
        print("\n\n⚠️  INTERRUPCIÓN DETECTADA - Finalizando y guardando progreso...")
        self._interrupted = True
    
    async def initialize(self) -> int:
        """Inicializa el scraper con configuración óptima. Retorna cantidad de RUCs."""
        
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║       OSIPTEL SCRAPER v1.0 - MODO 100% AUTOMÁTICO                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        
        # 1. Detectar recursos del sistema
        self.resources = await self.optimizer.optimize()
        
        # Sobrescribir workers si es modo manual
        if self.manual_workers is not None:
            print(f"\n🔧 MODO MANUAL: Configurando {self.manual_workers} workers")
            self.resources.optimal_workers = self.manual_workers
        
        self.optimizer.print_summary()
        
        # 2. Crear backup del archivo de RUCs
        self.file_manager.create_backup()
        
        # 3. Crear configuración optimizada
        self.config = create_optimized_config(
            resources=self.resources,
            max_bandwidth_mb=self.max_bandwidth_mb,
            output_dir=self.output_dir
        )
        
        # Crear directorio de salida
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 4. Contar RUCs
        df = pd.read_csv(self.input_file, dtype=str)
        ruc_column = None
        for col in df.columns:
            if 'ruc' in col.lower():
                ruc_column = col
                break
        if ruc_column is None:
            ruc_column = df.columns[0]
        
        rucs = df[ruc_column].astype(str).str.strip().tolist()
        rucs = [r for r in rucs if r and len(r) == 11 and r.isdigit()]
        
        self.report.total_rucs_to_process = len(rucs)
        
        print(f"\n📁 Archivo: {self.input_file}")
        print(f"📊 RUCs a procesar: {len(rucs):,}")
        print(f"💾 Bandwidth: ILIMITADO ♾️")
        print(f"📈 Todos los RUCs serán procesados sin restricción de bandwidth")
        
        return len(rucs)
    
    async def run(self):
        """Ejecuta el scraping de forma 100% automática"""
        
        # Inicializar
        ruc_count = await self.initialize()
        
        if ruc_count == 0:
            print("❌ No hay RUCs para procesar")
            return
        
        # Mostrar configuración
        print(f"\n⚡ CONFIGURACIÓN AUTO-OPTIMIZADA:")
        print(f"   Workers paralelos:  {self.resources.optimal_workers}")
        print(f"   Delays:             {self.resources.optimal_delay_min:.1f}-{self.resources.optimal_delay_max:.1f}s")
        print(f"   Reintentos máx:     2")
        
        # Iniciar reporte
        self.report.start_time = datetime.now()
        self.report.workers_used = self.resources.optimal_workers
        
        print(f"\n🚀 INICIANDO SCRAPING AUTOMÁTICO a las {self.report.start_time.strftime('%H:%M:%S')}...")
        print("   Presiona Ctrl+C para detener de forma segura\n")
        print("="*78)
        
        try:
            # Crear orchestrator y ejecutar
            self.orchestrator = ScraperOrchestrator(self.config, Path(self.input_file).stem)
            stats = await self.orchestrator.run(self.input_file)
            
            # Obtener resultados
            self.processed_rucs = self.orchestrator.progress_manager.processed_rucs.copy()
            
            # Actualizar reporte
            self.report.rucs_processed = stats.processed
            self.report.rucs_successful = stats.successful
            self.report.rucs_failed = stats.failed
            self.report.bandwidth_used_mb = stats.bandwidth_mb
            self.report.finish_reason = "completed"
            
        except asyncio.CancelledError:
            self.report.finish_reason = "interrupted"
        except Exception as e:
            self.logger.error(f"Error durante scraping: {e}")
            self.report.finish_reason = f"error: {str(e)}"
            import traceback
            traceback.print_exc()
        finally:
            await self.finalize()
    
    async def finalize(self):
        """Finaliza el scraping y genera reportes"""
        
        self.report.end_time = datetime.now()
        
        # Calcular métricas
        self.report.total_rucs_to_process = self.report.total_rucs_to_process or self.report.rucs_processed
        self.report.rucs_remaining = max(0, self.report.total_rucs_to_process - self.report.rucs_processed)
        self.report.bandwidth_remaining_mb = self.max_bandwidth_mb - self.report.bandwidth_used_mb
        
        self.report.calculate_metrics()
        
        # Imprimir reporte
        self.report.print_report()
        
        # Actualizar archivo de RUCs (eliminar procesados)
        if self.processed_rucs and len(self.processed_rucs) > 0:
            print("\n📝 Actualizando archivo de RUCs (eliminando procesados)...")
            removed, remaining = self.file_manager.remove_processed_rucs(self.processed_rucs)
            print(f"   ✓ RUCs eliminados del archivo: {removed:,}")
            print(f"   ✓ RUCs restantes por procesar: {remaining:,}")
        
        # Guardar reporte en JSON
        report_file = os.path.join(self.output_dir, "performance_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.report.to_dict(), f, indent=2, ensure_ascii=False, default=str)
        print(f"\n📄 Reporte guardado en: {report_file}")
        
        # Resumen final
        print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                              RESUMEN FINAL                                   ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ✓ RUCs procesados: {self.report.rucs_processed:>10,}                                         ║
║  ✓ Exitosos:        {self.report.rucs_successful:>10,}                                         ║
║  ✗ Fallidos:        {self.report.rucs_failed:>10,}                                         ║
║  ⏳ Restantes:       {self.report.rucs_remaining:>10,}                                         ║
║  ⏱️  Velocidad:       {self.report.rucs_per_hour:>10,.0f} RUCs/hora                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        
        if self.report.rucs_remaining > 0:
            print(f"💡 Para continuar con los RUCs restantes, ejecuta de nuevo:")
            print(f"   python run_auto.py {self.input_file}")


async def main():
    """Función principal"""
    
    # Menú de selección de modo
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║       OSIPTEL SCRAPER v1.0 - MODO DE EJECUCIÓN                               ║
╚══════════════════════════════════════════════════════════════════════════════╝

Selecciona el modo de configuración de workers:

  [1] Automático - Analizar PC y asignar workers automáticamente
  [2] Manual     - Especificar cantidad de workers manualmente

╔══════════════════════════════════════════════════════════════════════════════╗
""")
    
    while True:
        try:
            choice = input("Selecciona una opción (1 o 2): ").strip()
            if choice in ['1', '2']:
                break
            print("❌ Opción inválida. Ingresa 1 o 2.")
        except EOFError:
            print("\n❌ Entrada cancelada")
            sys.exit(1)
    
    manual_workers = None
    if choice == '2':
        while True:
            try:
                workers_input = input("\nIngresa la cantidad de workers (1-50): ").strip()
                manual_workers = int(workers_input)
                if 1 <= manual_workers <= 50:
                    break
                print("❌ La cantidad debe estar entre 1 y 50")
            except ValueError:
                print("❌ Ingresa un número válido")
            except EOFError:
                print("\n❌ Entrada cancelada")
                sys.exit(1)
    
    print("\n")
    
    parser = argparse.ArgumentParser(
        description='OSIPTEL Scraper v1.0 - Modo 100% Automático',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
    python run_auto.py rucs/RUCS_a_scrapear.csv
    python run_auto.py rucs/RUCS_a_scrapear.csv --bandwidth 10000
    python run_auto.py rucs/RUCS_a_scrapear.csv -b 5000 -o resultados/
        """
    )
    
    parser.add_argument(
        'input_file',
        help='Archivo CSV con los RUCs a scrapear'
    )
    
    parser.add_argument(
        '--bandwidth', '-b',
        type=float,
        default=10000,
        help='Bandwidth máximo en MB (default: 10000 = 10GB)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='osiptel_output',
        help='Directorio de salida (default: osiptel_output)'
    )
    
    args = parser.parse_args()
    
    # Validar archivo
    if not os.path.exists(args.input_file):
        print(f"❌ Error: Archivo no encontrado: {args.input_file}")
        sys.exit(1)
    
    # Setup logging básico
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Crear y ejecutar scraper
    scraper = AutoScraper(
        input_file=args.input_file,
        max_bandwidth_mb=args.bandwidth,
        output_dir=args.output,
        manual_workers=manual_workers
    )
    
    await scraper.run()


if __name__ == "__main__":
    # Windows usa ProactorEventLoop por defecto, que soporta subprocess
    # NO usar WindowsSelectorEventLoopPolicy porque no soporta subprocess_exec
    asyncio.run(main())
