import csv
import json
import requests
import time
import os
import pickle
import glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

class ProgressTracker:
    def __init__(self, filename='progress.pkl'):
        self.filename = filename
        self.completed = set()
        self.failed = []
        self.lock = Lock()
        self.total_processed = 0
        self.load_progress()
    
    def load_progress(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'rb') as f:
                    data = pickle.load(f)
                    self.completed = data.get('completed', set())
                    self.failed = data.get('failed', [])
                    self.total_processed = len(self.completed)
                    print(f"✅ Resumed: {self.total_processed} already completed")
            except:
                print("⚠️ Could not load progress file, starting fresh")
    
    def save_progress(self):
        with self.lock:
            with open(self.filename, 'wb') as f:
                pickle.dump({
                    'completed': self.completed,
                    'failed': self.failed,
                    'timestamp': datetime.now().isoformat()
                }, f)
    
    def mark_completed(self, url):
        with self.lock:
            self.completed.add(url)
            self.total_processed += 1
            
            # Save every 500 completions
            if self.total_processed % 500 == 0:
                self.save_progress()
                print(f"💾 Progress saved: {self.total_processed} completed")
    
    def mark_failed(self, url, error):
        with self.lock:
            self.failed.append({'url': url, 'error': str(error), 'time': datetime.now().isoformat()})
    
    def is_completed(self, url):
        with self.lock:
            return url in self.completed

def process_webhook(row_data, progress):
    url, method, payload, header = row_data
    
    # Skip if already processed
    if progress.is_completed(url):
        return {'status': 'skipped', 'url': url}
    
    try:
        # Prepare headers
        headers = {}
        if header:
            try:
                headers = json.loads(header)
            except:
                headers = {}
        
        # Add timeout and retry logic
        for attempt in range(3):
            try:
                if method.upper() == "POST" and payload:
                    response = requests.post(
                        url, 
                        json=json.loads(payload) if payload else {},
                        headers=headers,
                        timeout=30
                    )
                else:
                    response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    progress.mark_completed(url)
                    return {'status': 'success', 'url': url}
                elif attempt == 2:  # Last attempt
                    error = f"Status code: {response.status_code}"
                    progress.mark_failed(url, error)
                    return {'status': 'failed', 'url': url, 'error': error}
                    
            except requests.exceptions.Timeout:
                if attempt == 2:
                    progress.mark_failed(url, "Timeout")
                    return {'status': 'failed', 'url': url, 'error': 'Timeout'}
            except Exception as e:
                if attempt == 2:
                    progress.mark_failed(url, str(e))
                    return {'status': 'failed', 'url': url, 'error': str(e)}
            
            # Wait before retry
            if attempt < 2:
                time.sleep(1)
                
    except Exception as e:
        progress.mark_failed(url, str(e))
        return {'status': 'failed', 'url': url, 'error': str(e)}

def read_csv_files():
    """Read all http_triggers CSV files"""
    webhooks = []
    
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
        print("❌ No CSV files found!")
        return webhooks
    
    print(f"📄 Found {len(existing_files)} CSV file(s)")
    
    # Process each file
    for csv_file in existing_files:
        print(f"📖 Reading {csv_file}...")
        
        # Try different encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                with open(csv_file, 'r', encoding=encoding, errors='ignore') as f:
                    reader = csv.DictReader(f)
                    file_count = 0
                    
                    for row in reader:
                        url = row.get('webhook_url', '').strip()
                        if url:
                            webhooks.append((
                                url,
                                row.get('method', 'GET').strip() or 'GET',
                                row.get('payload', '').strip(),
                                row.get('header', '').strip()
                            ))
                            file_count += 1
                    
                    print(f"  ✅ Read {file_count} webhooks from {csv_file}")
                    break
            except Exception as e:
                if encoding == 'cp1252':  # Last encoding attempt
                    print(f"  ❌ Error reading {csv_file}: {e}")
                continue
    
    return webhooks

def main():
    print("=" * 50)
    print("🚀 LeadSquared Webhook Trigger")
    print("=" * 50)
    
    # Configuration
    MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '10'))
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '5000'))
    
    # Initialize progress tracker
    progress = ProgressTracker()
    
    # Read all webhooks from all CSV files
    print("📖 Reading CSV files...")
    all_webhooks = read_csv_files()
    
    if not all_webhooks:
        print("❌ No webhooks found in CSV files!")
        return
    
    # Filter out already completed
    webhooks_to_process = [w for w in all_webhooks if not progress.is_completed(w[0])]
    
    print(f"\n📊 Statistics:")
    print(f"  Total webhooks found: {len(all_webhooks):,}")
    print(f"  Already completed: {len(progress.completed):,}")
    print(f"  To be processed: {len(webhooks_to_process):,}")
    print(f"  Failed (will retry): {len(progress.failed):,}")
    
    if not webhooks_to_process:
        print("\n✅ All webhooks already processed!")
        return
    
    print(f"\n⚙️  Configuration:")
    print(f"  Workers: {MAX_WORKERS}")
    print(f"  Batch size: {BATCH_SIZE}")
    
    # Process in batches
    total_batches = (len(webhooks_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\n🔄 Starting processing in {total_batches} batches...")
    
    start_time = time.time()
    success_count = 0
    failed_count = 0
    
    for batch_num in range(0, len(webhooks_to_process), BATCH_SIZE):
        batch = webhooks_to_process[batch_num:batch_num + BATCH_SIZE]
        current_batch = (batch_num // BATCH_SIZE) + 1
        
        print(f"\n📦 Processing batch {current_batch}/{total_batches} ({len(batch)} webhooks)")
        
        batch_start = time.time()
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_webhook, webhook, progress) for webhook in batch]
    
    batch_success = 0
    batch_failed = 0
    completed_in_batch = 0
    
    for future in as_completed(futures):
        try:
            result = future.result()
            completed_in_batch += 1
            
            # Print progress every 100 completions
            if completed_in_batch % 100 == 0:
                print(f"  📊 Batch progress: {completed_in_batch}/{len(batch)}")
            
            if result['status'] == 'success':
                batch_success += 1
                success_count += 1
            elif result['status'] == 'failed':
                batch_failed += 1
                failed_count += 1
        except Exception as e:
            print(f"❌ Executor error: {e}")
            failed_count += 1
            
        batch_time = time.time() - batch_start
        rate = len(batch) / batch_time if batch_time > 0 else 0
        
        print(f"  ✅ Success: {batch_success}")
        print(f"  ❌ Failed: {batch_failed}")
        print(f"  ⏱️  Time: {batch_time:.1f}s ({rate:.1f} webhooks/sec)")
        
        # Save progress after each batch
        progress.save_progress()
        
        # Estimate remaining time
        if batch_num > 0:
            elapsed = time.time() - start_time
            avg_rate = success_count / elapsed
            remaining = len(webhooks_to_process) - (batch_num + len(batch))
            eta = remaining / avg_rate if avg_rate > 0 else 0
            print(f"  ⏳ ETA: {eta/60:.1f} minutes")
        
        # Small delay between batches
        if current_batch < total_batches:
            time.sleep(2)
    
    # Final save
    progress.save_progress()
    
    # Final report
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("📊 FINAL REPORT")
    print("=" * 50)
    print(f"✅ Total Successful: {len(progress.completed):,}")
    print(f"❌ Total Failed: {len(progress.failed):,}")
    print(f"⏱️  Total Time: {total_time/60:.1f} minutes")
    print(f"📈 Average Rate: {len(progress.completed)/total_time:.1f} webhooks/sec")
    
    # Save detailed failed report
    if progress.failed:
        with open('failed_webhooks.json', 'w') as f:
            json.dump(progress.failed, f, indent=2)
        print(f"\n💾 Failed webhooks saved to: failed_webhooks.json")
    
    print("\n✅ Process completed!")

if __name__ == "__main__":
    main()
