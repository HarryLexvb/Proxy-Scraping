"""
================================================================================
OSIPTEL SCRAPER - PRODUCTION VERSION 3.0
Part 3: Main Orchestrator
================================================================================
"""

import asyncio
import signal
import sys
import os
from datetime import datetime
from typing import List, Optional
import logging

from osiptel_core import (
    ScraperConfig, ProxyConfig, TaskStatus, RUCResult,
    Statistics, ProgressManager, ResultsWriter,
    read_rucs_from_file, validate_ruc_count, setup_logging
)
from osiptel_worker import Worker, ProxyManager


# ============================================================================
# ORCHESTRATOR
# ============================================================================

class ScraperOrchestrator:
    """
    Main orchestrator that coordinates all workers and manages the scraping process.
    
    Features:
    - Parallel processing with configurable workers
    - Automatic resume from interruptions
    - Real-time statistics and progress tracking
    - Bandwidth monitoring
    - Graceful shutdown on CTRL+C
    """
    
    def __init__(self, config: ScraperConfig, input_filename: str = "rucs"):
        self.config = config
        self.logger = setup_logging(config)
        self._input_filename = input_filename
        
        # Managers
        self.proxy_manager = ProxyManager(config.proxy)
        self.progress_manager = ProgressManager(config)
        self.results_writer = ResultsWriter(config, input_filename)
        
        # Workers
        self.workers: List[Worker] = []
        self.worker_tasks: List[asyncio.Task] = []
        
        # Queues
        self.ruc_queue: Optional[asyncio.Queue] = None
        self.result_queue: Optional[asyncio.Queue] = None
        
        # State
        self._shutdown_requested = False
        self._result_processor_task: Optional[asyncio.Task] = None
    
    async def _delayed_worker_start(self, worker: Worker, delay: float, worker_id: int):
        """
        Start a worker after a random delay.
        This creates staggered starts to avoid detection by OSIPTEL.
        Makes requests look like natural traffic from different users.
        """
        if delay > 0:
            self.logger.debug(f"Worker {worker_id} esperando {delay:.1f}s antes de iniciar...")
            await asyncio.sleep(delay)
        return await worker.run()
    
    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            if not self._shutdown_requested:
                self.logger.warning("\n⚠️  Interrupción detectada. Guardando progreso...")
                self._shutdown_requested = True
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def _result_callback(self, result: RUCResult):
        """Callback when a worker completes a RUC"""
        await self.result_queue.put(result)
    
    async def _process_results(self):
        """Process results from workers"""
        checkpoint_counter = 0
        
        while True:
            try:
                result = await asyncio.wait_for(
                    self.result_queue.get(),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                if self._shutdown_requested or all(not t.done() for t in self.worker_tasks):
                    continue
                break
            
            if result is None:
                break
            
            # Estimate bandwidth for this request
            # With images blocked: ~400-600 KB
            # We use 550 KB as conservative estimate
            estimated_kb = self.config.estimated_kb_per_ruc
            
            # Update progress
            self.progress_manager.add_result(result, estimated_kb)
            
            # Write to CSV
            if result.success:
                await self.results_writer.write_result(result)
            
            checkpoint_counter += 1
            
            # Periodic checkpoint
            if checkpoint_counter >= self.config.checkpoint_interval:
                await self.progress_manager.save()
                checkpoint_counter = 0
                
                # Log progress with bandwidth info
                stats = self.progress_manager.statistics
                remaining_gb = (self.config.max_bandwidth_mb - stats.bandwidth_mb) / 1024
                used_gb = stats.bandwidth_mb / 1024
                total_gb = self.config.max_bandwidth_mb / 1024
                
                self.logger.info(
                    f"📊 Progreso: {stats.processed:,}/{stats.total_rucs:,} "
                    f"({stats.success_rate:.1f}% éxito) | "
                    f"BW: {used_gb:.2f} GB usado | "
                    f"ETA: {stats.eta_seconds/60:.0f} min"
                )
            
            # Bandwidth limit check disabled - unlimited bandwidth mode
            # The scraper will continue until all RUCs are processed
            
            self.result_queue.task_done()
    
    async def run(self, file_path: str, resume: bool = True) -> Statistics:
        """
        Run the scraping process.
        
        Args:
            file_path: Path to Excel/CSV file with RUCs
            resume: Whether to resume from previous progress
            
        Returns:
            Statistics object with results
        """
        self._setup_signal_handlers()
        
        # Create output directory
        os.makedirs(self.config.output_dir, exist_ok=True)
        
        # Validate proxy configuration
        if not self.config.proxy.is_configured():
            self.logger.error("❌ Error: Credenciales de proxy no configuradas")
            self.logger.error("   Configura PROXY_USER y PROXY_PASS en el archivo de configuración")
            return Statistics()
        
        # Read RUCs from file
        self.logger.info(f"📂 Leyendo archivo: {file_path}")
        try:
            all_rucs, total_rows = read_rucs_from_file(file_path)
        except Exception as e:
            self.logger.error(f"❌ Error leyendo archivo: {e}")
            return Statistics()
        
        self.logger.info(f"   Filas en archivo: {total_rows:,}")
        self.logger.info(f"   RUCs válidos (11 dígitos): {len(all_rucs):,}")
        
        if not all_rucs:
            self.logger.error("❌ No se encontraron RUCs válidos")
            return Statistics()
        
        # Try to load previous progress
        if resume and await self.progress_manager.load():
            prev_stats = self.progress_manager.statistics
            self.logger.info(f"🔄 Reanudando sesión anterior:")
            self.logger.info(f"   Ya procesados: {prev_stats.processed:,}")
            self.logger.info(f"   Bandwidth usado: {prev_stats.bandwidth_mb:.1f} MB")
        
        # Get pending RUCs
        pending_rucs = self.progress_manager.get_pending_rucs(all_rucs)
        
        if not pending_rucs:
            self.logger.info("✅ Todos los RUCs ya fueron procesados!")
            return self.progress_manager.statistics
        
        # Bandwidth is unlimited - no restrictions
        self.logger.info(f"📊 Análisis de capacidad:")
        self.logger.info(f"   RUCs pendientes: {len(pending_rucs):,}")
        self.logger.info(f"   Modo bandwidth: ILIMITADO ♾️")
        self.logger.info(f"   Todos los RUCs serán procesados sin restricción")
        
        # Update total in statistics
        self.progress_manager.statistics.total_rucs = len(all_rucs)
        
        if self.progress_manager.statistics.start_time is None:
            self.progress_manager.statistics.start_time = datetime.now()
        
        # Initialize results writer
        await self.results_writer.initialize()
        
        # Create queues
        self.ruc_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        
        # Fill RUC queue
        for ruc in pending_rucs:
            await self.ruc_queue.put(ruc)
        
        # Add poison pills for workers
        for _ in range(self.config.max_workers):
            await self.ruc_queue.put(None)
        
        self.logger.info(f"🚀 Iniciando {self.config.max_workers} workers con inicio escalonado...")
        self.logger.info(f"   Procesando {len(pending_rucs):,} RUCs")
        
        # Start result processor
        self._result_processor_task = asyncio.create_task(self._process_results())
        
        # Create and start workers with STAGGERED START
        # Each worker starts with a random delay to avoid detection
        # This creates a natural distribution of requests over time
        import random
        
        for i in range(self.config.max_workers):
            worker = Worker(
                worker_id=i,
                config=self.config,
                proxy_manager=self.proxy_manager,
                ruc_queue=self.ruc_queue,
                result_callback=self._result_callback
            )
            self.workers.append(worker)
            
            # Staggered start: each worker waits a random delay before starting
            # Workers 0-3: start immediately to 2s delay
            # Workers 4-7: 2-5s delay  
            # Workers 8+: 4-8s delay
            # This creates "waves" of workers that look like natural traffic
            if i < 4:
                delay = random.uniform(0, 2.0)
            elif i < 8:
                delay = random.uniform(2.0, 5.0)
            else:
                delay = random.uniform(4.0, 8.0)
            
            task = asyncio.create_task(self._delayed_worker_start(worker, delay, i))
            self.worker_tasks.append(task)
        
        # Wait for completion or shutdown
        try:
            while not self._shutdown_requested:
                # Check if all workers are done
                if all(task.done() for task in self.worker_tasks):
                    break
                
                await asyncio.sleep(1)
            
            # If shutdown requested, stop workers gracefully
            if self._shutdown_requested:
                self.logger.info("Deteniendo workers...")
                for worker in self.workers:
                    await worker.stop()
                
                # Wait a bit for graceful shutdown
                await asyncio.sleep(3)
                
                # Cancel remaining tasks
                for task in self.worker_tasks:
                    if not task.done():
                        task.cancel()
            
            # Wait for worker tasks
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            
        except asyncio.CancelledError:
            self.logger.info("Proceso cancelado")
        
        finally:
            # Stop result processor
            await self.result_queue.put(None)
            if self._result_processor_task:
                try:
                    await asyncio.wait_for(self._result_processor_task, timeout=10)
                except:
                    self._result_processor_task.cancel()
            
            # Finalize batch saving (save any remaining results)
            await self.results_writer.finalize()
            
            # Final save
            self.progress_manager.statistics.end_time = datetime.now()
            await self.progress_manager.save()
            
            # Save failed RUCs
            if self.progress_manager.failed_rucs:
                await self.results_writer.write_failed_rucs(
                    self.progress_manager.failed_rucs
                )
            
            # Save statistics
            stats_path = self.config.get_output_path(self.config.stats_file)
            with open(stats_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(
                    self.progress_manager.statistics.to_dict(),
                    f,
                    indent=2,
                    ensure_ascii=False
                )
            
            # Print summary
            self.progress_manager.statistics.print_summary()
            
            self.logger.info(f"📁 Resultados guardados en: {self.config.output_dir}/")
            self.logger.info(f"   - {self.config.results_file}")
            self.logger.info(f"   - {self.config.batch_save_dir}/ (archivos por lote)")
            if self.progress_manager.failed_rucs:
                self.logger.info(f"   - {self.config.failed_file}")
            self.logger.info(f"   - {self.config.stats_file}")
            self.logger.info(f"   - {self.config.progress_file}")
        
        return self.progress_manager.statistics


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point with interactive menu"""
    
    print("\n" + "="*70)
    print("       OSIPTEL SCRAPER - PRODUCTION VERSION 3.0")
    print("       Extractor de Líneas Telefónicas por RUC")
    print("="*70)
    
    # Load or create configuration
    config = ScraperConfig()
    
    # Menu
    print("\nOpciones:")
    print("  1. Iniciar/Reanudar scraping")
    print("  2. Ver progreso actual")
    print("  3. Configurar credenciales de proxy")
    print("  4. Calcular RUCs máximos para bandwidth")
    print("  5. Salir")
    print()
    
    try:
        option = input("Selecciona opción (1-5): ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nSaliendo...")
        return
    
    if option == "1":
        # Check proxy configuration
        if not config.proxy.is_configured():
            print("\n⚠️  Proxy no configurado. Ingresa las credenciales:")
            config.proxy.username = input("Usuario SmartProxy: ").strip()
            config.proxy.password = input("Contraseña SmartProxy: ").strip()
            
            if not config.proxy.is_configured():
                print("❌ Credenciales requeridas")
                return
        
        # Get file path
        try:
            file_path = input("\nRuta del archivo (Excel o CSV): ").strip()
            file_path = file_path.strip('"').strip("'")
        except (KeyboardInterrupt, EOFError):
            print("\nCancelado")
            return
        
        if not os.path.exists(file_path):
            print(f"❌ Archivo no encontrado: {file_path}")
            return
        
        # Ask for workers
        print(f"\nWorkers por defecto: {config.max_workers}")
        try:
            workers_input = input("Número de workers (Enter para default): ").strip()
            if workers_input:
                config.max_workers = max(1, min(20, int(workers_input)))
        except:
            pass
        
        # Run
        orchestrator = ScraperOrchestrator(config)
        await orchestrator.run(file_path)
    
    elif option == "2":
        progress_manager = ProgressManager(config)
        if await progress_manager.load():
            stats = progress_manager.statistics
            print("\n📊 Progreso Actual:")
            print(f"   Total RUCs: {stats.total_rucs:,}")
            print(f"   Procesados: {stats.processed:,}")
            print(f"   Exitosos: {stats.successful:,}")
            print(f"   Fallidos: {stats.failed:,}")
            print(f"   Tasa de éxito: {stats.success_rate:.1f}%")
            print(f"   Bandwidth usado: {stats.bandwidth_mb:.1f} MB")
            print(f"   Bandwidth restante: {config.max_bandwidth_mb - stats.bandwidth_mb:.1f} MB")
        else:
            print("\n❌ No hay progreso guardado")
    
    elif option == "3":
        print("\n📝 Configuración de Proxy SmartProxy")
        print("-"*50)
        config.proxy.username = input("Usuario: ").strip()
        config.proxy.password = input("Contraseña: ").strip()
        
        use_peru = input("¿Usar IPs de Perú? (s/n): ").strip().lower()
        config.proxy.use_peru_ips = use_peru == 's'
        
        print("\n✅ Configuración guardada")
        print("   Ahora selecciona opción 1 para iniciar")
    
    elif option == "4":
        print("\n📊 Calculadora de RUCs Máximos")
        print("-"*50)
        
        try:
            bandwidth_mb = float(input("Bandwidth disponible (MB): ").strip())
        except:
            bandwidth_mb = 10000
        
        # Calculations
        conservative_kb = 650  # With retries
        optimistic_kb = 450   # Optimal conditions
        
        max_conservative = int((bandwidth_mb * 1024) / conservative_kb)
        max_optimistic = int((bandwidth_mb * 1024) / optimistic_kb)
        recommended = int((max_conservative + max_optimistic) / 2)
        
        print(f"\nPara {bandwidth_mb:.0f} MB de bandwidth:")
        print(f"   Conservador (con reintentos): {max_conservative:,} RUCs")
        print(f"   Optimista (sin errores):      {max_optimistic:,} RUCs")
        print(f"   ✓ RECOMENDADO:                {recommended:,} RUCs")
        print(f"\nPara 10 GB (10,000 MB):")
        print(f"   ✓ NÚMERO EXACTO RECOMENDADO:  15,000 RUCs")
    
    else:
        print("\nSaliendo...")


if __name__ == "__main__":
    asyncio.run(main())
