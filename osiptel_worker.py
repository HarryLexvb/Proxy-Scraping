"""
================================================================================
OSIPTEL SCRAPER - PRODUCTION VERSION 3.0
Part 2: Proxy Manager and Worker Implementation
================================================================================
"""

import asyncio
import random
import time
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import logging

from osiptel_core import (
    ScraperConfig, ProxyConfig, TaskStatus, ErrorType,
    RUCResult, PhoneLine, Statistics
)

# Camoufox import
from camoufox.async_api import AsyncCamoufox


# ============================================================================
# PROXY MANAGER
# ============================================================================

class ProxyManager:
    """
    Manages SmartProxy residential proxy connections.
    
    SmartProxy rotation works by session ID - each unique session gets a new IP.
    We generate unique session IDs for each worker and optionally for each request.
    
    Your proxy format: smart-haroldvpn_area-PE (already has Peru area configured)
    """
    
    def __init__(self, config: ProxyConfig):
        self.config = config
        self._session_counter = 0
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger('osiptel.proxy')
    
    async def get_session_id(self, worker_id: int) -> str:
        """Generate a unique session ID for proxy rotation"""
        async with self._lock:
            self._session_counter += 1
            timestamp = int(time.time() * 1000) % 100000
            random_part = random.randint(10000, 99999)
            return f"w{worker_id}s{self._session_counter}t{timestamp}r{random_part}"
    
    def get_proxy_config_for_camoufox(self, session_id: str) -> Dict[str, str]:
        """
        Get proxy configuration for Camoufox browser.
        
        IMPORTANT: SmartProxy uses underscore (_) for parameters, NOT hyphen (-)!
        Correct format: username_session-XXXXX_area-PE
        
        Using hyphen (-session-) breaks geo-targeting and gives IPs from random countries.
        """
        base_username = self.config.username  # smart-haroldvpn_area-PE
        
        # Append session AFTER _area-PE using underscore format
        # Format: smart-haroldvpn_area-PE_session-XXXXX
        username_with_session = f"{base_username}_session-{session_id}"
        
        return {
            "server": f"http://{self.config.host}:{self.config.port}",
            "username": username_with_session,
            "password": self.config.password
        }
    
    def get_proxy_url(self, session_id: str) -> str:
        """Get full proxy URL for logging/debugging"""
        base_username = self.config.username
        username_with_session = f"{base_username}_session-{session_id}"
        return f"http://{username_with_session}@{self.config.host}:{self.config.port}"


# ============================================================================
# BROWSER MANAGER
# ============================================================================

class BrowserManager:
    """
    Manages browser lifecycle with proper error handling and resource cleanup.
    """
    
    def __init__(
        self,
        worker_id: int,
        config: ScraperConfig,
        proxy_manager: ProxyManager
    ):
        self.worker_id = worker_id
        self.config = config
        self.proxy_manager = proxy_manager
        self.logger = logging.getLogger(f'osiptel.browser.{worker_id}')
        
        self._browser = None
        self._browser_context = None
        self._current_session_id: Optional[str] = None
        self._pages_opened = 0
        self._max_pages_per_browser = 150  # Restart browser after N pages (más páginas = menos overhead)
    
    async def _create_browser(self) -> bool:
        """Create a new browser instance with proxy"""
        try:
            # Get new session ID for IP rotation
            self._current_session_id = await self.proxy_manager.get_session_id(self.worker_id)
            proxy_config = self.proxy_manager.get_proxy_config_for_camoufox(self._current_session_id)
            
            self.logger.debug(f"Creating browser with session: {self._current_session_id}")
            
            # Create Camoufox browser with optimizations
            self._browser_context = AsyncCamoufox(
                headless=self.config.headless,
                humanize=self.config.humanize,
                proxy=proxy_config,
                block_images=self.config.block_images,  # CRITICAL: Saves ~40% bandwidth
                i_know_what_im_doing=True,  # Suppress block_images warning
                geoip=False,  # OPTIMIZADO: Ahorra ~200KB/browser (proxy ya tiene Peru)
            )
            
            self._browser = await self._browser_context.__aenter__()
            self._pages_opened = 0
            
            self.logger.debug("Browser created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create browser: {e}")
            await self._cleanup()
            return False
    
    async def _cleanup(self):
        """Clean up browser resources"""
        if self._browser_context:
            try:
                await self._browser_context.__aexit__(None, None, None)
            except Exception as e:
                self.logger.debug(f"Browser cleanup error (ignored): {e}")
            finally:
                self._browser = None
                self._browser_context = None
    
    async def get_browser(self, force_new: bool = False):
        """Get browser instance, creating new one if needed"""
        # Check if we need to restart browser
        needs_restart = (
            force_new or
            self._browser is None or
            self._pages_opened >= self._max_pages_per_browser
        )
        
        if needs_restart:
            await self._cleanup()
            if not await self._create_browser():
                raise RuntimeError("Failed to create browser")
        
        return self._browser
    
    async def new_page(self):
        """Create a new page with the current browser"""
        browser = await self.get_browser()
        page = await browser.new_page()
        self._pages_opened += 1
        return page
    
    async def rotate_ip(self):
        """Force IP rotation by restarting browser with new session"""
        self.logger.debug("Rotating IP...")
        await self._cleanup()
        await self._create_browser()
    
    async def close(self):
        """Close browser and cleanup"""
        await self._cleanup()
        self.logger.debug("Browser closed")


# ============================================================================
# OSIPTEL PAGE SCRAPER
# ============================================================================

class OSIPTELPageScraper:
    """
    Handles the actual scraping logic for OSIPTEL page.
    Separated from Worker for better testability and maintainability.
    """
    
    OSIPTEL_URL = "https://checatuslineas.osiptel.gob.pe/"
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = logging.getLogger('osiptel.scraper')
    
    async def scrape_ruc(self, page, ruc: str) -> RUCResult:
        """
        Scrape phone lines for a single RUC.
        
        Args:
            page: Playwright page object
            ruc: 11-digit RUC number
            
        Returns:
            RUCResult with status and data
        """
        start_time = time.time()
        
        try:
            # Navigate to OSIPTEL - use networkidle like v1 that worked
            await page.goto(
                self.OSIPTEL_URL,
                timeout=120000,  # 2 minutes for slow proxy
                wait_until='networkidle'  # Changed from domcontentloaded
            )
            
            # Wait for form to be ready
            await page.wait_for_selector('#IdTipoDoc', timeout=30000)
            
            # Random human-like delay (optimizado)
            await page.wait_for_timeout(random.randint(1000, 2000))
            
            # Select document type (RUC = 2)
            await page.evaluate('document.getElementById("IdTipoDoc").value = "2"')
            await page.wait_for_timeout(random.randint(200, 500))
            
            # Enter RUC number
            await page.evaluate(f'document.getElementById("NumeroDocumento").value = "{ruc}"')
            await page.wait_for_timeout(random.randint(200, 500))
            
            # Click search button
            await page.evaluate('document.getElementById("btnBuscar").click()')
            
            # Wait for initial table load (ultra-robusto para evitar falsos negativos)
            await page.wait_for_timeout(10000)
            
            # Try to change page size to get all results
            await self._set_page_size(page)
            
            # Wait for table update and verify complete load
            await self._wait_for_table_complete(page)
            
            # Extract data with cuádruple validation to eliminate false negatives
            data = await self._extract_table_data(page)
            
            # Cuádruple validation: if suspicious result, retry up to 4 times
            retry_count = 0
            max_retries = 4
            
            while retry_count < max_retries:
                # Check if result is suspicious (0 lines but table exists)
                if len(data) == 0:
                    table_info = await page.evaluate('''() => {
                        const table = document.getElementById("GridConsulta");
                        if (!table) return {exists: false, rows: 0};
                        
                        let visibleRows = 0;
                        for (let i = 1; i < table.rows.length; i++) {
                            const row = table.rows[i];
                            if (!row.className || !row.className.includes('GridPager')) {
                                if (row.style.display !== 'none') visibleRows++;
                            }
                        }
                        return {exists: true, rows: table.rows.length, visible: visibleRows};
                    }''')
                    
                    # If table has visible rows but we got 0 data, it's a false negative
                    if table_info['exists'] and table_info['visible'] > 0:
                        retry_count += 1
                        # Wait longer and re-extract
                        await page.wait_for_timeout(5000)
                        data = await self._extract_table_data(page)
                        continue
                
                # Data looks valid, exit
                break
            
            # EMERGENCY CHECK: If still 0 after all retries but table has data, force one final extraction
            if len(data) == 0 and retry_count >= max_retries:
                final_check = await page.evaluate('''() => {
                    const table = document.getElementById("GridConsulta");
                    if (!table) return {hasData: false};
                    
                    for (let i = 1; i < table.rows.length; i++) {
                        const row = table.rows[i];
                        if (!row.className || !row.className.includes('GridPager')) {
                            const cells = row.cells;
                            if (cells && cells.length > 0) {
                                const text = cells[0]?.innerText?.trim();
                                if (text && text.length > 0 && !text.includes('Cargando')) {
                                    return {hasData: true};
                                }
                            }
                        }
                    }
                    return {hasData: false};
                }''')
                
                if final_check['hasData']:
                    # One final desperate attempt
                    await page.wait_for_timeout(8000)
                    data = await self._extract_table_data(page)
            
            duration = time.time() - start_time
            
            # Build result
            lines = []
            for row in data:
                if len(row) >= 3:
                    lines.append(PhoneLine(
                        modalidad=row[0],
                        numero=row[1],
                        operadora=row[2]
                    ))
            
            return RUCResult(
                ruc=ruc,
                status=TaskStatus.SUCCESS,
                lines=lines,
                duration_seconds=duration
            )
            
        except asyncio.TimeoutError:
            return RUCResult(
                ruc=ruc,
                status=TaskStatus.FAILED,
                error_type=ErrorType.TIMEOUT,
                error_message="Timeout waiting for page/selector",
                duration_seconds=time.time() - start_time
            )
            
        except Exception as e:
            error_type = self._classify_error(str(e))
            return RUCResult(
                ruc=ruc,
                status=TaskStatus.FAILED,
                error_type=error_type,
                error_message=str(e)[:500],
                duration_seconds=time.time() - start_time
            )
    
    async def _wait_for_table_complete(self, page):
        """Wait for table to load completely with ultra-robust stability check to eliminate false negatives"""
        max_attempts = 30  # 30 × 2s = 60s máximo (ultra-robusto)
        stable_count = 0
        prev_count = -1
        
        for attempt in range(max_attempts):
            await page.wait_for_timeout(2000)  # 2s intervalos
            
            # Get current row count with better validation
            current_count = await page.evaluate('''() => {
                const table = document.getElementById("GridConsulta");
                if (!table) return -1;
                
                let validRows = 0;
                for (let i = 1; i < table.rows.length; i++) {
                    const row = table.rows[i];
                    
                    // Skip pager and hidden rows
                    if (row.className && row.className.includes('GridPager')) continue;
                    if (row.style.display === 'none' || row.style.visibility === 'hidden') continue;
                    
                    const cells = row.cells;
                    if (!cells || cells.length < 3) continue;
                    
                    const text = cells[0].innerText?.trim();
                    if (text && text !== '' && 
                        !text.includes('Cargando') && 
                        !text.includes('Loading') &&
                        !text.includes('No se encontraron')) {
                        validRows++;
                    }
                }
                return validRows;
            }''')
            
            # Check if count is stable (need 4 consecutive stable checks for ultra-robustness)
            if current_count == prev_count and current_count >= 0:
                stable_count += 1
                if stable_count >= 4:  # 4 checks consecutivos estables (ultra-robusto)
                    break
            else:
                stable_count = 0
            
            prev_count = current_count
        
        # Final wait for any pending renders
        await page.wait_for_timeout(3000)
    
    async def _set_page_size(self, page):
        """Try to set page size to 100 to get all records"""
        try:
            await page.evaluate('''() => {
                const selector = document.querySelector('select[name="GridConsulta_length"]');
                if (selector) {
                    // Add 100 option if not exists
                    let hasOption = false;
                    for (let option of selector.options) {
                        if (option.value === '100') {
                            hasOption = true;
                            break;
                        }
                    }
                    if (!hasOption) {
                        const newOption = document.createElement('option');
                        newOption.value = '100';
                        newOption.text = '100';
                        selector.add(newOption);
                    }
                    selector.value = '100';
                    selector.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }''')
        except Exception:
            pass  # Non-critical, continue with default page size
    
    async def _extract_table_data(self, page) -> list:
        """Extract data from results table with robust validation"""
        return await page.evaluate('''() => {
            const table = document.getElementById("GridConsulta");
            if (!table) return [];
            
            const rows = table.rows;
            const data = [];
            
            for (let i = 1; i < rows.length; i++) {
                const row = rows[i];
                
                // Skip pager row
                if (row.className && row.className.includes('GridPager')) {
                    continue;
                }
                
                // Skip hidden, loading, or placeholder rows
                if (row.style.display === 'none' || row.style.visibility === 'hidden') {
                    continue;
                }
                
                const cells = row.cells;
                if (!cells || cells.length < 3) continue;
                
                const rowData = [];
                let hasRealContent = false;
                
                for (let j = 0; j < cells.length; j++) {
                    const cellText = cells[j].innerText ? cells[j].innerText.trim() : '';
                    rowData.push(cellText);
                    if (cellText && cellText.length > 0) hasRealContent = true;
                }
                
                // Only add rows with valid, real data (not loading/empty messages)
                if (hasRealContent && rowData.length >= 3 && rowData[0] && rowData[0] !== '' && 
                    !rowData[0].includes('Cargando') && 
                    !rowData[0].includes('Loading') &&
                    !rowData[0].includes('No se encontraron') &&
                    !rowData[0].includes('Procesando')) {
                    data.push(rowData);
                }
            }
            
            return data;
        }''')
    
    def _classify_error(self, error_message: str) -> ErrorType:
        """Classify error based on message"""
        error_lower = error_message.lower()
        
        if 'timeout' in error_lower:
            return ErrorType.TIMEOUT
        elif 'proxy' in error_lower or 'connect' in error_lower:
            return ErrorType.PROXY_ERROR
        elif 'selector' in error_lower or 'element' in error_lower:
            return ErrorType.SELECTOR_NOT_FOUND
        elif 'navigation' in error_lower or 'net::' in error_lower:
            return ErrorType.PAGE_LOAD_ERROR
        elif 'crash' in error_lower or 'target closed' in error_lower:
            return ErrorType.BROWSER_CRASH
        elif '429' in error_lower or 'rate' in error_lower or 'banned' in error_lower:
            return ErrorType.RATE_LIMITED
        else:
            return ErrorType.UNKNOWN


# ============================================================================
# WORKER
# ============================================================================

class Worker:
    """
    Individual worker that processes RUCs from a queue.
    Each worker has its own browser instance and handles retries.
    """
    
    def __init__(
        self,
        worker_id: int,
        config: ScraperConfig,
        proxy_manager: ProxyManager,
        ruc_queue: asyncio.Queue,
        result_callback
    ):
        self.worker_id = worker_id
        self.config = config
        self.proxy_manager = proxy_manager
        self.ruc_queue = ruc_queue
        self.result_callback = result_callback
        
        self.logger = logging.getLogger(f'osiptel.worker.{worker_id}')
        self.browser_manager = BrowserManager(worker_id, config, proxy_manager)
        self.page_scraper = OSIPTELPageScraper(config)
        
        self._running = True
        self._processed_count = 0
    
    async def stop(self):
        """Signal worker to stop"""
        self._running = False
    
    async def run(self):
        """Main worker loop"""
        self.logger.info(f"Worker {self.worker_id} started")
        
        try:
            while self._running:
                try:
                    # Get next RUC from queue with timeout
                    ruc = await asyncio.wait_for(
                        self.ruc_queue.get(),
                        timeout=30.0
                    )
                except asyncio.TimeoutError:
                    # No more RUCs, check if we should continue
                    if self.ruc_queue.empty():
                        break
                    continue
                
                # None is the poison pill to stop
                if ruc is None:
                    self.ruc_queue.task_done()
                    break
                
                # Process the RUC with retries
                result = await self._process_ruc_with_retries(ruc)
                
                # Send result
                await self.result_callback(result)
                
                self._processed_count += 1
                self.ruc_queue.task_done()
                
                # Delay entre requests (optimizado - sin coffee breaks)
                delay = random.uniform(
                    self.config.min_delay_between_requests,
                    self.config.max_delay_between_requests
                )
                await asyncio.sleep(delay)
                
        except asyncio.CancelledError:
            self.logger.info(f"Worker {self.worker_id} cancelled")
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} crashed: {e}")
        finally:
            await self.browser_manager.close()
            self.logger.info(f"Worker {self.worker_id} stopped. Processed: {self._processed_count}")
    
    async def _process_ruc_with_retries(self, ruc: str) -> RUCResult:
        """Process a RUC with retry logic"""
        last_result = None
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Get/create browser
                page = await self.browser_manager.new_page()
                page_closed = False
                
                try:
                    # Scrape
                    result = await self.page_scraper.scrape_ruc(page, ruc)
                    result.attempts = attempt
                    
                    if result.success:
                        if result.line_count > 0:
                            self.logger.info(f"✓ RUC {ruc}: {result.line_count} líneas")
                        else:
                            self.logger.info(f"✓ RUC {ruc}: Sin líneas registradas")
                        return result
                    
                    last_result = result
                    
                except Exception as inner_e:
                    # Page might be closed already if browser crashed
                    if 'closed' in str(inner_e).lower():
                        page_closed = True
                    raise
                finally:
                    # Only close page if it's still open
                    if not page_closed:
                        try:
                            await page.close()
                        except Exception:
                            pass  # Ignore close errors
                
            except Exception as e:
                error_msg = str(e)
                # Determine error type
                if 'closed' in error_msg.lower() or 'target' in error_msg.lower():
                    error_type = ErrorType.BROWSER_CRASH
                elif 'timeout' in error_msg.lower():
                    error_type = ErrorType.TIMEOUT
                else:
                    error_type = ErrorType.UNKNOWN
                    
                last_result = RUCResult(
                    ruc=ruc,
                    status=TaskStatus.FAILED,
                    error_type=error_type,
                    error_message=error_msg[:500],
                    attempts=attempt
                )
            
            # Prepare for retry
            if attempt < self.config.max_retries:
                # Calculate backoff delay
                delay = min(
                    self.config.retry_base_delay * (self.config.retry_multiplier ** (attempt - 1)),
                    self.config.retry_max_delay
                )
                delay += random.uniform(0, delay * 0.3)  # Add jitter
                
                self.logger.warning(
                    f"⟳ RUC {ruc}: Reintento {attempt + 1}/{self.config.max_retries} "
                    f"en {delay:.1f}s - {last_result.error_type.value if last_result.error_type else 'error'}"
                )
                
                await asyncio.sleep(delay)
                
                # Rotate IP for retry
                await self.browser_manager.rotate_ip()
        
        # All retries failed
        self.logger.error(f"✗ RUC {ruc}: Falló después de {self.config.max_retries} intentos")
        last_result.attempts = self.config.max_retries
        return last_result
