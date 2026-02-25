"""
================================================================================
OSIPTEL SCRAPER - PRODUCTION VERSION 3.0
================================================================================
Enterprise-grade scraper with residential proxy support for OSIPTEL database.
Optimized for bandwidth efficiency and reliability.

Author: Harold Alejandro Villanueva Borda
License: MIT
Version: 3.0.0 (Production)

BANDWIDTH CALCULATION:
- Estimated consumption per RUC: 400-600 KB (with optimizations)
- 10 GB limit = 10,240 MB = 10,485,760 KB
- Safe RUC limit for 10GB: 15,000 RUCs (conservative, includes retries)
- Maximum theoretical: 20,000 RUCs (no retries, optimal conditions)

RECOMMENDED: Start with 12,000-15,000 RUCs for first production run.
================================================================================
"""

import asyncio
import json
import os
import random
import logging
import hashlib
import signal
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple, Any
from enum import Enum
from contextlib import asynccontextmanager
import traceback

# Third-party imports
import pandas as pd
import aiofiles
from aiofiles import os as aio_os

# Camoufox import
try:
    from camoufox.async_api import AsyncCamoufox
except ImportError:
    print("ERROR: camoufox not installed. Run: pip install camoufox && python -m camoufox fetch")
    sys.exit(1)


# ============================================================================
# CONSTANTS AND ENUMS
# ============================================================================

class TaskStatus(Enum):
    """Status of a RUC scraping task"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ErrorType(Enum):
    """Types of errors that can occur"""
    TIMEOUT = "timeout"
    PROXY_ERROR = "proxy_error"
    PAGE_LOAD_ERROR = "page_load_error"
    SELECTOR_NOT_FOUND = "selector_not_found"
    DATA_EXTRACTION_ERROR = "data_extraction_error"
    BROWSER_CRASH = "browser_crash"
    RATE_LIMITED = "rate_limited"
    UNKNOWN = "unknown"


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ProxyConfig:
    """SmartProxy configuration"""
    host: str = "proxy.smartproxy.net"  # Your SmartProxy server
    port: int = 3120  # Your SmartProxy port
    username: str = "smart-haroldvpn_area-PE"  # Your username with Peru area
    password: str = "LXxRWv414OBG6tVW"  # Your password
    
    # For Peru-specific IPs (already configured in username with _area-PE)
    use_peru_ips: bool = False
    peru_port: int = 3120  # Same port
    
    def get_effective_port(self) -> int:
        return self.port  # Always use configured port
    
    def is_configured(self) -> bool:
        return bool(self.username and self.password)


@dataclass
class ScraperConfig:
    """
    Main scraper configuration.
    
    BANDWIDTH CALCULATION FOR 10GB:
    ===============================
    With all optimizations enabled (block_images, minimal wait times):
    - Average request: ~400-600 KB per RUC
    - With 20% retry overhead: ~550 KB per RUC
    - 10GB = 10,240 MB = ~18,600 RUCs theoretical max
    - SAFE LIMIT: 15,000 RUCs (leaves 17% buffer)
    
    Without optimizations:
    - Average request: ~800 KB - 1.2 MB per RUC
    - 10GB = ~8,500-12,800 RUCs
    """
    
    # === PROXY ===
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    
    # === PARALLELIZATION ===
    max_workers: int = 12  # Optimal for i7-1255U with residential proxies
    
    # === BANDWIDTH LIMITS ===
    max_bandwidth_mb: int = 9700  # Leave 300MB buffer from 10GB
    estimated_kb_per_ruc: float = 550.0  # Conservative estimate with retries
    warn_bandwidth_percent: float = 80.0  # Warn at 80% usage
    
    # === SAFE RUC LIMITS ===
    # For 10GB plan: max_rucs = 9700 MB / 0.55 MB = ~17,600
    # But we use 15,000 as safe limit with margin
    max_rucs_for_10gb: int = 15000
    
    # === TIMEOUTS (milliseconds) ===
    page_timeout: int = 45000  # 45 seconds for page load
    selector_timeout: int = 20000  # 20 seconds for selectors
    navigation_timeout: int = 60000  # 60 seconds total navigation
    
    # === DELAYS (seconds) ===
    min_delay_between_requests: float = 1.5
    max_delay_between_requests: float = 4.0
    delay_after_error: float = 3.0
    
    # === RETRY CONFIGURATION ===
    max_retries: int = 3
    retry_base_delay: float = 5.0
    retry_max_delay: float = 30.0
    retry_multiplier: float = 2.0
    
    # === BROWSER OPTIMIZATION ===
    block_images: bool = True  # CRITICAL: Saves ~40% bandwidth
    headless: bool = True
    humanize: bool = True
    
    # === FILE PATHS ===
    output_dir: str = "osiptel_output"
    results_file: str = "resultados.csv"
    progress_file: str = "progress.json"
    failed_file: str = "failed_rucs.csv"
    log_file: str = "scraper.log"
    stats_file: str = "statistics.json"
    
    # === CHECKPOINT ===
    checkpoint_interval: int = 25  # Save every N RUCs
    
    # === BATCH SAVE CONFIGURATION ===
    batch_save_size: int = 1000  # Save partial file every N RUCs
    batch_save_dir: str = "save"  # Directory for batch saves
    
    # === BANDWIDTH TRACKING ===
    track_bandwidth: bool = True
    
    def calculate_max_rucs(self) -> int:
        """Calculate maximum RUCs based on bandwidth limit"""
        max_kb = self.max_bandwidth_mb * 1024
        return int(max_kb / self.estimated_kb_per_ruc)
    
    def get_output_path(self, filename: str) -> str:
        return os.path.join(self.output_dir, filename)


# ============================================================================
# LOGGING SETUP
# ============================================================================

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def setup_logging(config: ScraperConfig) -> logging.Logger:
    """Configure logging system"""
    os.makedirs(config.output_dir, exist_ok=True)
    
    logger = logging.getLogger('osiptel')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # File handler (detailed)
    file_handler = logging.FileHandler(
        config.get_output_path(config.log_file),
        encoding='utf-8',
        mode='a'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler (colored)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# ============================================================================
# DATA CLASSES FOR RESULTS
# ============================================================================

@dataclass
class PhoneLine:
    """Single phone line record"""
    modalidad: str
    numero: str
    operadora: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class RUCResult:
    """Result of scraping a single RUC"""
    ruc: str
    status: TaskStatus
    lines: List[PhoneLine] = field(default_factory=list)
    error_type: Optional[ErrorType] = None
    error_message: Optional[str] = None
    attempts: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    duration_seconds: float = 0.0
    
    @property
    def success(self) -> bool:
        return self.status == TaskStatus.SUCCESS
    
    @property
    def line_count(self) -> int:
        return len(self.lines)
    
    def to_dict(self) -> Dict:
        return {
            'ruc': self.ruc,
            'status': self.status.value,
            'line_count': self.line_count,
            'error_type': self.error_type.value if self.error_type else None,
            'error_message': self.error_message,
            'attempts': self.attempts,
            'timestamp': self.timestamp,
            'duration_seconds': self.duration_seconds
        }


# ============================================================================
# STATISTICS TRACKER
# ============================================================================

@dataclass
class Statistics:
    """Real-time statistics tracking"""
    total_rucs: int = 0
    processed: int = 0
    successful: int = 0
    failed: int = 0
    total_lines_found: int = 0
    estimated_bandwidth_kb: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    # Error breakdown
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    
    # Retry statistics
    total_retries: int = 0
    
    def update(self, result: RUCResult, estimated_kb: float):
        """Update statistics with a new result"""
        self.processed += 1
        self.estimated_bandwidth_kb += estimated_kb
        
        if result.success:
            self.successful += 1
            self.total_lines_found += result.line_count
        else:
            self.failed += 1
            if result.error_type:
                error_name = result.error_type.value
                self.errors_by_type[error_name] = self.errors_by_type.get(error_name, 0) + 1
        
        self.total_retries += max(0, result.attempts - 1)
    
    @property
    def success_rate(self) -> float:
        if self.processed == 0:
            return 0.0
        return (self.successful / self.processed) * 100
    
    @property
    def bandwidth_mb(self) -> float:
        return self.estimated_bandwidth_kb / 1024
    
    @property
    def elapsed_seconds(self) -> float:
        if not self.start_time:
            return 0.0
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
    
    @property
    def rate_per_hour(self) -> float:
        elapsed_hours = self.elapsed_seconds / 3600
        if elapsed_hours == 0:
            return 0.0
        return self.processed / elapsed_hours
    
    @property
    def eta_seconds(self) -> float:
        """Estimated time remaining"""
        if self.processed == 0 or self.rate_per_hour == 0:
            return 0.0
        remaining = self.total_rucs - self.processed
        return (remaining / self.rate_per_hour) * 3600
    
    def to_dict(self) -> Dict:
        return {
            'total_rucs': self.total_rucs,
            'processed': self.processed,
            'successful': self.successful,
            'failed': self.failed,
            'success_rate': round(self.success_rate, 2),
            'total_lines_found': self.total_lines_found,
            'estimated_bandwidth_mb': round(self.bandwidth_mb, 2),
            'elapsed_seconds': round(self.elapsed_seconds, 1),
            'rate_per_hour': round(self.rate_per_hour, 1),
            'eta_seconds': round(self.eta_seconds, 1),
            'errors_by_type': self.errors_by_type,
            'total_retries': self.total_retries,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None
        }
    
    def print_summary(self):
        """Print formatted summary to console"""
        print("\n" + "="*70)
        print("                    RESUMEN DE EJECUCIÓN")
        print("="*70)
        print(f"  Total RUCs en archivo:      {self.total_rucs:,}")
        print(f"  Procesados:                 {self.processed:,}")
        print(f"  Exitosos:                   {self.successful:,}")
        print(f"  Fallidos:                   {self.failed:,}")
        print(f"  Tasa de éxito:              {self.success_rate:.1f}%")
        print(f"  Líneas telefónicas:         {self.total_lines_found:,}")
        print("-"*70)
        print(f"  Bandwidth estimado:         {self.bandwidth_mb:.2f} MB")
        print(f"  Tiempo total:               {self.elapsed_seconds/60:.1f} minutos")
        print(f"  Velocidad:                  {self.rate_per_hour:.0f} RUCs/hora")
        print(f"  Reintentos totales:         {self.total_retries:,}")
        
        if self.errors_by_type:
            print("-"*70)
            print("  Desglose de errores:")
            for error_type, count in sorted(self.errors_by_type.items(), key=lambda x: -x[1]):
                print(f"    - {error_type}: {count}")
        
        print("="*70 + "\n")


# ============================================================================
# PROGRESS MANAGER
# ============================================================================

class ProgressManager:
    """Manages progress persistence for resume capability"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.progress_path = config.get_output_path(config.progress_file)
        self._lock = asyncio.Lock()
        
        # Progress data
        self.processed_rucs: Set[str] = set()
        self.failed_rucs: Dict[str, Dict] = {}  # {ruc: {error_type, error_message, attempts}}
        self.statistics = Statistics()
        
    async def load(self) -> bool:
        """Load previous progress. Returns True if progress was loaded."""
        if not os.path.exists(self.progress_path):
            return False
        
        try:
            async with aiofiles.open(self.progress_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                data = json.loads(content)
            
            self.processed_rucs = set(data.get('processed_rucs', []))
            self.failed_rucs = data.get('failed_rucs', {})
            
            # Restore statistics
            stats_data = data.get('statistics', {})
            self.statistics = Statistics(
                total_rucs=stats_data.get('total_rucs', 0),
                processed=stats_data.get('processed', 0),
                successful=stats_data.get('successful', 0),
                failed=stats_data.get('failed', 0),
                total_lines_found=stats_data.get('total_lines_found', 0),
                estimated_bandwidth_kb=stats_data.get('estimated_bandwidth_mb', 0) * 1024,
                errors_by_type=stats_data.get('errors_by_type', {}),
                total_retries=stats_data.get('total_retries', 0)
            )
            
            if stats_data.get('start_time'):
                self.statistics.start_time = datetime.fromisoformat(stats_data['start_time'])
            
            return True
            
        except Exception as e:
            logging.getLogger('osiptel').warning(f"Could not load progress: {e}")
            return False
    
    async def save(self):
        """Save current progress to disk"""
        async with self._lock:
            data = {
                'processed_rucs': list(self.processed_rucs),
                'failed_rucs': self.failed_rucs,
                'statistics': self.statistics.to_dict(),
                'last_save': datetime.now().isoformat()
            }
            
            # Write to temp file first, then rename (atomic operation)
            temp_path = self.progress_path + '.tmp'
            async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            
            # Atomic rename
            os.replace(temp_path, self.progress_path)
    
    def add_result(self, result: RUCResult, estimated_kb: float):
        """Add a result to progress tracking"""
        self.processed_rucs.add(result.ruc)
        self.statistics.update(result, estimated_kb)
        
        if not result.success:
            self.failed_rucs[result.ruc] = {
                'error_type': result.error_type.value if result.error_type else 'unknown',
                'error_message': result.error_message,
                'attempts': result.attempts
            }
    
    def get_pending_rucs(self, all_rucs: List[str]) -> List[str]:
        """Get RUCs that haven't been processed yet"""
        return [ruc for ruc in all_rucs if ruc not in self.processed_rucs]
    
    def is_bandwidth_exceeded(self) -> bool:
        """Check if bandwidth limit is exceeded"""
        # MODIFICADO: Bandwidth ilimitado - siempre retorna False
        return False
    
    def get_remaining_bandwidth_mb(self) -> float:
        """Get remaining bandwidth in MB"""
        return max(0, self.config.max_bandwidth_mb - self.statistics.bandwidth_mb)


# ============================================================================
# CSV WRITER
# ============================================================================

class ResultsWriter:
    """Writes results to CSV incrementally with batch saving support"""
    
    def __init__(self, config: ScraperConfig, input_filename: str = "rucs"):
        self.config = config
        self.output_path = config.get_output_path(config.results_file)
        self._lock = asyncio.Lock()
        self._header_written = False
        self._max_lines = 0
        
        # Batch save configuration
        self._batch_results: List[RUCResult] = []
        self._total_saved = 0
        self._batch_number = 0
        self._input_filename = Path(input_filename).stem  # Get filename without extension
        self._batch_start_time = datetime.now()
        self._batch_times: List[Dict] = []  # Store timing info for each batch
        
        # Create save directory
        self._save_dir = os.path.join(config.output_dir, config.batch_save_dir)
        os.makedirs(self._save_dir, exist_ok=True)
    
    async def initialize(self):
        """Initialize the CSV file"""
        # Check if file exists and has content
        if os.path.exists(self.output_path):
            try:
                df = pd.read_csv(self.output_path, nrows=1)
                if len(df.columns) > 1:
                    self._header_written = True
                    # Count existing line columns
                    for col in df.columns:
                        if col.startswith('Modalidad_'):
                            num = int(col.split('_')[1])
                            self._max_lines = max(self._max_lines, num)
            except:
                pass
    
    def _generate_header(self, max_lines: int) -> List[str]:
        """Generate CSV header with indexed columns"""
        header = ['RUC']
        for i in range(1, max_lines + 1):
            header.extend([
                f'Modalidad_{i}',
                f'Numero_Telefonico_{i}',
                f'Empresa_Operadora_{i}'
            ])
        return header
    
    async def write_result(self, result: RUCResult):
        """Write a single result to CSV and handle batch saving"""
        async with self._lock:
            # Fixed 100 columns
            self._max_lines = 100
            
            # Add to batch
            self._batch_results.append(result)
            
            # Prepare row
            row_data = {'RUC': result.ruc}
            
            for idx, line in enumerate(result.lines, 1):
                row_data[f'Modalidad_{idx}'] = line.modalidad
                row_data[f'Numero_Telefonico_{idx}'] = line.numero
                row_data[f'Empresa_Operadora_{idx}'] = line.operadora
            
            # Write to main file
            write_header = not self._header_written
            
            async with aiofiles.open(self.output_path, 'a', encoding='utf-8-sig', newline='') as f:
                if write_header:
                    header = self._generate_header(100)
                    await f.write(','.join(header) + '\n')
                    self._header_written = True
                
                # Build row string
                values = [str(row_data.get('RUC', ''))]
                for i in range(1, 101):
                    values.extend([
                        self._escape_csv(str(row_data.get(f'Modalidad_{i}', ''))),
                        self._escape_csv(str(row_data.get(f'Numero_Telefonico_{i}', ''))),
                        self._escape_csv(str(row_data.get(f'Empresa_Operadora_{i}', '')))
                    ])
                
                await f.write(','.join(values) + '\n')
            
            # Check if we need to save a batch
            if len(self._batch_results) >= self.config.batch_save_size:
                await self._save_batch()
    
    async def _save_batch(self):
        """Save current batch to a separate file"""
        if not self._batch_results:
            return
        
        self._batch_number += 1
        batch_end_time = datetime.now()
        batch_duration = (batch_end_time - self._batch_start_time).total_seconds()
        
        # Generate batch filename: name_1000_parte1.csv
        batch_filename = f"{self._input_filename}_{self.config.batch_save_size}_parte{self._batch_number}.csv"
        batch_path = os.path.join(self._save_dir, batch_filename)
        
        # Fixed 100 columns for batch
        batch_max_lines = 100
        
        # Write batch file
        async with aiofiles.open(batch_path, 'w', encoding='utf-8-sig', newline='') as f:
            # Write header
            header = self._generate_header(100)
            await f.write(','.join(header) + '\n')
            
            # Write all results in batch
            for result in self._batch_results:
                row_data = {'RUC': result.ruc}
                for idx, line in enumerate(result.lines, 1):
                    row_data[f'Modalidad_{idx}'] = line.modalidad
                    row_data[f'Numero_Telefonico_{idx}'] = line.numero
                    row_data[f'Empresa_Operadora_{idx}'] = line.operadora
                
                values = [str(row_data.get('RUC', ''))]
                for i in range(1, 101):
                    values.extend([
                        self._escape_csv(str(row_data.get(f'Modalidad_{i}', ''))),
                        self._escape_csv(str(row_data.get(f'Numero_Telefonico_{i}', ''))),
                        self._escape_csv(str(row_data.get(f'Empresa_Operadora_{i}', '')))
                    ])
                await f.write(','.join(values) + '\n')
        
        # Record timing info
        successful_count = sum(1 for r in self._batch_results if r.success)
        total_lines = sum(r.line_count for r in self._batch_results)
        
        self._batch_times.append({
            'batch_number': self._batch_number,
            'filename': batch_filename,
            'rucs_count': len(self._batch_results),
            'successful_count': successful_count,
            'total_lines': total_lines,
            'start_time': self._batch_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': batch_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_seconds': round(batch_duration, 2),
            'duration_formatted': str(timedelta(seconds=int(batch_duration))),
            'rucs_per_minute': round(len(self._batch_results) / (batch_duration / 60), 2) if batch_duration > 0 else 0
        })
        
        # Update timing report
        await self._save_timing_report()
        
        # Update totals and reset batch
        self._total_saved += len(self._batch_results)
        self._batch_results = []
        self._batch_start_time = datetime.now()
        
        # Log batch save
        logging.getLogger('osiptel').info(
            f"💾 Lote {self._batch_number} guardado: {batch_filename} "
            f"({successful_count} RUCs, {total_lines} líneas, {batch_duration:.1f}s)"
        )
    
    async def _save_timing_report(self):
        """Save timing report to JSON file"""
        report_path = os.path.join(self._save_dir, f"{self._input_filename}_timing_report.json")
        
        # Calculate totals
        total_rucs = sum(b['rucs_count'] for b in self._batch_times)
        total_successful = sum(b['successful_count'] for b in self._batch_times)
        total_lines = sum(b['total_lines'] for b in self._batch_times)
        total_duration = sum(b['duration_seconds'] for b in self._batch_times)
        
        report = {
            'summary': {
                'total_batches': len(self._batch_times),
                'total_rucs_processed': total_rucs,
                'total_successful': total_successful,
                'total_phone_lines': total_lines,
                'total_duration_seconds': round(total_duration, 2),
                'total_duration_formatted': str(timedelta(seconds=int(total_duration))),
                'average_rucs_per_minute': round(total_rucs / (total_duration / 60), 2) if total_duration > 0 else 0,
                'average_batch_duration': round(total_duration / len(self._batch_times), 2) if self._batch_times else 0,
                'success_rate': round((total_successful / total_rucs * 100), 2) if total_rucs > 0 else 0
            },
            'batches': self._batch_times
        }
        
        async with aiofiles.open(report_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(report, indent=2, ensure_ascii=False))
    
    async def finalize(self):
        """Save any remaining results in the batch"""
        async with self._lock:
            if self._batch_results:
                await self._save_batch()
            
            # Final timing report
            if self._batch_times:
                await self._save_timing_report()
                logging.getLogger('osiptel').info(
                    f"📊 Reporte de tiempos guardado en: {self._save_dir}/{self._input_filename}_timing_report.json"
                )
    
    def _escape_csv(self, value: str) -> str:
        """Escape CSV field if needed"""
        if ',' in value or '"' in value or '\n' in value:
            return f'"{value.replace(chr(34), chr(34)+chr(34))}"'
        return value
    
    async def write_failed_rucs(self, failed_rucs: Dict[str, Dict]):
        """Write failed RUCs to separate file"""
        if not failed_rucs:
            return
        
        path = self.config.get_output_path(self.config.failed_file)
        async with aiofiles.open(path, 'w', encoding='utf-8-sig') as f:
            await f.write('RUC,Error_Type,Error_Message,Attempts\n')
            for ruc, info in failed_rucs.items():
                error_msg = (info.get('error_message') or '').replace('"', "'").replace('\n', ' ')[:200]
                await f.write(f'{ruc},{info.get("error_type", "")},"{error_msg}",{info.get("attempts", 0)}\n')


# ============================================================================
# RUC FILE READER
# ============================================================================

def read_rucs_from_file(file_path: str) -> Tuple[List[str], int]:
    """
    Read RUCs from Excel or CSV file.
    Returns: (list of valid RUCs, total rows read)
    """
    file_ext = Path(file_path).suffix.lower()
    
    if file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path, dtype=str)
    elif file_ext == '.csv':
        df = pd.read_csv(file_path, dtype=str)
    else:
        raise ValueError(f"Formato no soportado: {file_ext}. Use .xlsx, .xls o .csv")
    
    total_rows = len(df)
    
    # Get first column
    rucs = df.iloc[:, 0].astype(str).str.strip().tolist()
    
    # Validate RUCs (must be 11 digits)
    valid_rucs = []
    for ruc in rucs:
        # Remove any non-digit characters
        clean_ruc = ''.join(filter(str.isdigit, ruc))
        if len(clean_ruc) == 11:
            valid_rucs.append(clean_ruc)
    
    return valid_rucs, total_rows


def validate_ruc_count(rucs: List[str], config: ScraperConfig, logger: logging.Logger) -> bool:
    """
    Validate RUC count against bandwidth limits.
    Returns True if within limits, False otherwise.
    """
    count = len(rucs)
    max_safe = config.calculate_max_rucs()
    bandwidth_needed_mb = (count * config.estimated_kb_per_ruc) / 1024
    
    logger.info(f"RUCs en archivo: {count:,}")
    logger.info(f"Máximo seguro para {config.max_bandwidth_mb} MB: {max_safe:,}")
    logger.info(f"Bandwidth estimado necesario: {bandwidth_needed_mb:.1f} MB")
    
    if count > max_safe:
        logger.warning(
            f"⚠️  ADVERTENCIA: {count:,} RUCs excede el límite seguro de {max_safe:,} "
            f"para {config.max_bandwidth_mb} MB"
        )
        logger.warning(
            f"Se procesarán los primeros {max_safe:,} RUCs. "
            f"Para más, necesitas un plan con más datos."
        )
        return False
    
    return True
