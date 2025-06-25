import csv
import json
import requests
import time
import argparse
import sys
import os
import glob
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
from tqdm import tqdm
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook_trigger.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, rate_limit, max_concurrent):
        self.rate_limit = rate_limit  # Initial rate limit in seconds
        self.lock = Lock()
        self.last_request_time = 0
        self.semaphore = Semaphore(max_concurrent)
        
    def wait_if_needed(self):
        """Ensures proper rate limiting between requests"""
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_request_time
            if time_since_last < self.rate_limit:
                time.sleep(self.rate_limit - time_since_last)
            self.last_request_time = time.time()
    
    def adjust_rate(self, new_rate_limit):
        with self.lock:
            self.rate_limit = new_rate_limit
            logger.info(f"Rate limit adjusted to {new_rate_limit:.2f} seconds")

    def get_rate(self):
        with self.lock:
            return self.rate_limit

def make_request(url, method, payload, header, retries, rate_limiter, pbar, results_tracker):
    """
    Makes an HTTP request with retry and rate limiting.
    After printing the API response (success or error), it immediately updates the shared tqdm progress bar.
    Returns 0 on success, 1 on failure.
    """
    # Acquire semaphore for concurrent request limiting
    with rate_limiter.semaphore:
        # Wait according to rate limit
        rate_limiter.wait_if_needed()
        
        start_time = time.time()
        
        for attempt in range(retries):
            try:
                # Determine HTTP method
                if not method:
                    method_to_use = "POST" if payload else "GET"
                else:
                    method_to_use = method.upper()
                
                # Prepare headers
                headers = None
                if header:
                    try:
                        headers = json.loads(header)
                        if not isinstance(headers, dict):
                            logger.warning(f"Header for {url} is not a valid JSON object. Ignoring header.")
                            headers = None
                    except Exception as e:
                        logger.error(f"Error parsing header JSON for {url}: {e}")
                        headers = None

                # Execute the request with timeout
                request_timeout = 30
                if method_to_use == "POST":
                    if payload:
                        try:
                            data = json.loads(payload)
                        except Exception as e:
                            logger.error(f"Error parsing payload JSON for {url}: {e}")
                            data = payload
                        response = requests.post(url, json=data, headers=headers, timeout=request_timeout)
                    else:
                        response = requests.post(url, headers=headers, timeout=request_timeout)
                elif method_to_use == "GET":
                    response = requests.get(url, headers=headers, timeout=request_timeout)
                else:
                    if payload:
                        try:
                            data = json.loads(payload)
                        except Exception as e:
                            logger.error(f"Error parsing payload JSON for {url}: {e}")
                            data = payload
                        response = requests.request(method_to_use, url, json=data, headers=headers, timeout=request_timeout)
                    else:
                        response = requests.request(method_to_use, url, headers=headers, timeout=request_timeout)
                
                # Handle response
                if response.status_code == 200:
                    # Limit response output for large responses
                    response_preview = response.text[:200] + "..." if len(response.text) > 200 else response.text
                    logger.info(f"Success: {url}, Response: {response_preview}")
                    
                    # Track result
                    results_tracker.add_result({
                        'url': url,
                        'method': method_to_use,
                        'status': 'success',
                        'status_code': response.status_code,
                        'timestamp': datetime.now().isoformat(),
                        'response_time': time.time() - start_time,
                        'attempt': attempt + 1
                    })
                    
                    pbar.update(1)
                    return 0
                else:
                    logger.error(f"Error: {url}, Status Code: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.error(f"Timeout error for {url} (attempt {attempt + 1}/{retries})")
            except Exception as e:
                logger.error(f"Error: {url}, Exception: {e} (attempt {attempt + 1}/{retries})")
            
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retrying

        # If all retries fail
        results_tracker.add_result({
            'url': url,
            'method': method_to_use if 'method_to_use' in locals() else 'UNKNOWN',
            'status': 'failed',
            'status_code': response.status_code if 'response' in locals() else None,
            'timestamp': datetime.now().isoformat(),
            'response_time': time.time() - start_time,
            'attempt': retries
        })
        
        pbar.update(1)
        return 1

class ResultsTracker:
    def __init__(self):
        self.results = []
        self.lock = Lock()
    
    def add_result(self, result):
        with self.lock:
            self.results.append(result)
    
    def save_results(self, filename='webhook_results.json'):
        with self.lock:
            try:
                with open(filename, 'w') as f:
                    json.dump(self.results, f, indent=2)
                logger.info(f"Results saved to {filename}")
            except Exception as e:
                logger.error(f"Error saving results: {e}")

def read_csv_in_chunks(csv_file, chunk_size=1000, skip_rows=0):
    """Read CSV file in chunks for memory efficiency"""
    try:
        with open(csv_file, 'r', encoding='utf-8', errors='replace') as file:
            reader = csv.DictReader(file)
            chunk = []
            row_count = 0
            skipped_count = 0
            
            for row in reader:
                # Skip rows if needed
                if row_count < skip_rows:
                    row_count += 1
                    skipped_count += 1
                    continue
                    
                if 'webhook_url' in row and row['webhook_url'].strip():
                    webhook_data = (
                        row['webhook_url'].strip(),
                        row.get('method', '').strip() or None,
                        row.get('payload', '').strip() or None,
                        row.get('header', '').strip() or None
                    )
                    chunk.append(webhook_data)
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                row_count += 1
                
            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} rows from {csv_file}")
                
            if chunk:
                yield chunk
    except FileNotFoundError:
        logger.error(f"CSV file '{csv_file}' not found.")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def read_multiple_csv_files(chunk_size=1000, skip_rows=0):
    """Read multiple CSV files for Railway deployment"""
    # Look for your specific CSV files
    csv_files = [
        'http_triggers.csv',
        'http_triggers 2.csv',
        'http_triggers 3.csv', 
        'http_triggers 4.csv'
    ]
    
    # Find existing files
    existing_files = [f for f in csv_files if os.path.exists(f)]
    
    if not existing_files:
        logger.error("No CSV files found!")
        return
    
    logger.info(f"Found {len(existing_files)} CSV file(s): {existing_files}")
    
    # Track total rows to skip across files
    remaining_skip = skip_rows
    
    # Process each file
    for csv_file in existing_files:
        logger.info(f"Reading {csv_file}...")
        try:
            # Calculate how many rows to skip in this file
            current_skip = remaining_skip if remaining_skip > 0 else 0
            
            # Read chunks from this file
            chunks_read = 0
            for chunk in read_csv_in_chunks(csv_file, chunk_size, current_skip):
                yield chunk
                chunks_read += len(chunk)
                
            # Update remaining skip count
            # We need to know total rows in file to properly skip across files
            # For now, we'll apply skip only to first file
            remaining_skip = 0
            
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {e}")
            continue

def trigger_webhooks_parallel(csv_file, base_rate_limit, starting_rate_limit, max_rate_limit, 
                            window_size, error_threshold, max_workers, skip_rows=0):
    try:
        # Count total webhooks first
        total_requests = 0
        all_webhooks = []
        
        # For Railway deployment with multiple files
        if csv_file == "AUTO":
            logger.info(f"Reading webhooks from multiple CSV files... (skipping first {skip_rows} rows)")
            for chunk in read_multiple_csv_files(chunk_size=1000, skip_rows=skip_rows):
                all_webhooks.extend(chunk)
                total_requests += len(chunk)
        else:
            # Single file mode
            logger.info(f"Reading webhooks from {csv_file} (skipping first {skip_rows} rows)")
            for chunk in read_csv_in_chunks(csv_file, chunk_size=1000, skip_rows=skip_rows):
                all_webhooks.extend(chunk)
                total_requests += len(chunk)
        
        if not all_webhooks:
            logger.warning("No valid webhook URLs found in CSV.")
            return

        logger.info(f"Found {total_requests} webhook entries in CSV.")

        rate_limiter = RateLimiter(starting_rate_limit, max_workers)
        results_tracker = ResultsTracker()
        error_window = []

        logger.info(f"Triggering {total_requests} webhooks with up to {max_workers} parallel threads...\n")

        # Create a shared tqdm progress bar.
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=total_requests, desc="Progress", dynamic_ncols=True) as pbar:
                future_to_url = {
                    executor.submit(make_request, url, method, payload, header, 3, rate_limiter, pbar, results_tracker): url
                    for url, method, payload, header in all_webhooks
                }
                
                for future in as_completed(future_to_url):
                    try:
                        error = future.result()
                        error_window.append(error)
                        if len(error_window) > window_size:
                            error_window.pop(0)
                    except Exception as e:
                        url = future_to_url[future]
                        logger.error(f"Error processing {url}: {e}")

                    # Adjust rate limit every window_size responses
                    if pbar.n % window_size == 0 and pbar.n > 0 and error_window:
                        error_rate = sum(error_window) / len(error_window)
                        current_rate_limit = rate_limiter.get_rate()
                        
                        if error_rate > error_threshold:
                            new_rate_limit = min(max_rate_limit, current_rate_limit * 1.2)
                            rate_limiter.adjust_rate(new_rate_limit)
                            logger.info(f"Increasing rate limit to {new_rate_limit:.2f} sec (error rate {error_rate:.2%}).")
                        elif error_rate < error_threshold / 2:
                            new_rate_limit = max(base_rate_limit, current_rate_limit / 1.2)
                            rate_limiter.adjust_rate(new_rate_limit)
                            logger.info(f"Decreasing rate limit to {new_rate_limit:.2f} sec (error rate {error_rate:.2%}).")

        # Save results
        results_tracker.save_results()
        
        # Print summary
        successful = sum(1 for r in results_tracker.results if r['status'] == 'success')
        failed = sum(1 for r in results_tracker.results if r['status'] == 'failed')
        logger.info(f"\nFinished processing all webhooks. Success: {successful}, Failed: {failed}")
        
        sys.stdout.flush()

    except FileNotFoundError:
        logger.error(f"Error: CSV file '{csv_file}' not found.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def load_config():
    """Load configuration from environment variables or use defaults"""
    return {
        'BASE_RATE_LIMIT': float(os.getenv('BASE_RATE_LIMIT', '3')),
        'STARTING_RATE_LIMIT': float(os.getenv('STARTING_RATE_LIMIT', '3')),
        'MAX_RATE_LIMIT': float(os.getenv('MAX_RATE_LIMIT', '5.0')),
        'WINDOW_SIZE': int(os.getenv('WINDOW_SIZE', '20')),
        'ERROR_THRESHOLD': float(os.getenv('ERROR_THRESHOLD', '0.3')),
        'MAX_WORKERS': int(os.getenv('MAX_WORKERS', '3')),
        'SKIP_ROWS': int(os.getenv('SKIP_ROWS', '0'))  # New variable
    }

if __name__ == "__main__":
    # For Railway deployment, we'll use environment variables instead of command line args
    if os.getenv('RAILWAY_ENVIRONMENT'):
        # Railway deployment mode
        config = load_config()
        logger.info(f"Configuration: {config}")
        
        trigger_webhooks_parallel(
            "AUTO",  # Special flag to read multiple CSV files
            config['BASE_RATE_LIMIT'],
            config['STARTING_RATE_LIMIT'],
            config['MAX_RATE_LIMIT'],
            config['WINDOW_SIZE'],
            config['ERROR_THRESHOLD'],
            config['MAX_WORKERS'],
            config['SKIP_ROWS']  # Pass skip_rows parameter
        )
    else:
        # Local mode with command line arguments
        parser = argparse.ArgumentParser(description="Trigger webhooks with dynamic rate adjustment.")
        parser.add_argument("csv_file", help="Path to the CSV file containing webhook details.")
        parser.add_argument("--config", help="Path to JSON config file (optional)")
        args = parser.parse_args()

        # Load configuration
        config = load_config()
        
        # Override with config file if provided
        if args.config:
            try:
                with open(args.config, 'r') as f:
                                        file_config = json.load(f)
                    config.update(file_config)
                    logger.info(f"Loaded configuration from {args.config}")
            except Exception as e:
                logger.warning(f"Could not load config file: {e}. Using defaults.")
        
        logger.info(f"Configuration: {config}")
        
        trigger_webhooks_parallel(
            args.csv_file,
            config['BASE_RATE_LIMIT'],
            config['STARTING_RATE_LIMIT'],
            config['MAX_RATE_LIMIT'],
            config['WINDOW_SIZE'],
            config['ERROR_THRESHOLD'],
            config['MAX_WORKERS'],
            config['SKIP_ROWS']  # Pass skip_rows parameter
        )
