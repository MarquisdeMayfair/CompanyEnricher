#!/usr/bin/env python3
"""
Background Enricher for Favorite Companies
==========================================
Automatically enriches companies with favorite SIC codes in phases.

Developed on: Gaming PC (mrdea)

Process:
1. Directors (Companies House API) 
2. Website (free DNS lookup)
3. Emails & Phones (free web scraping)
4. Email enrichment (Hunter.io - paid, only if needed)

SIC Code Phases (in order):
  Phase 1: Accountants (69201, 69203) - PRIORITY
  Phase 2: Office Admin (82110)
  Phase 3: Management Consultancy (70229)
  Phase 4: Business Support (82990)

Auto-stops and uploads to Google Drive when:
  - A phase completes
  - Hunter.io credits are exhausted

Usage:
    Start:  python background_enricher.py
    Stop:   Ctrl+C
    
    With options:
    python background_enricher.py --limit 100      # Process max 100 companies
    python background_enricher.py --no-hunter      # Skip Hunter.io (free only)
    python background_enricher.py --dry-run        # Show what would be done
    
Rate Limits:
    - Companies House: 1 request/second (600/5min limit)
    - Hunter.io: 1 request/3 seconds (conservative)
    - Web scraping: 2 requests/second
"""

import os
import sys
import time
import signal
import sqlite3
import requests
import argparse
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), 'companies.db')
COMPANIES_HOUSE_API_KEY = os.getenv('COMPANIES_HOUSE_API_KEY')
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')

# SIC Code Phases - processed in order
# Phase 1: Accountants (PRIORITY)
# Phase 2: Office Admin
# Phase 3: Management Consultancy  
# Phase 4: Business Support
SIC_PHASES = [
    {'name': 'Accountants', 'codes': ['69201', '69203']},
    {'name': 'Office Admin', 'codes': ['82110']},
    {'name': 'Management Consultancy', 'codes': ['70229']},
    {'name': 'Business Support', 'codes': ['82990']},
]

# Current phase SIC codes (will be updated as phases complete)
FAVORITE_SICS = ['69201', '69203']  # Start with Phase 1

# Rate limiting (seconds between requests)
CH_DELAY = 1.0      # Companies House: 1 req/sec
HUNTER_DELAY = 3.0  # Hunter: 1 req/3sec (conservative)
SCRAPE_DELAY = 0.5  # Web scraping: 2 req/sec

# Enrichment status values
STATUS_NOT_ATTEMPTED = 'not_attempted'
STATUS_PARTIAL = 'partial'
STATUS_SUCCESS = 'success'
STATUS_FAILED = 'failed'

# Generic email prefixes - NOT personal emails, should still try Hunter
GENERIC_EMAIL_PREFIXES = [
    'info', 'hello', 'contact', 'enquiries', 'enquiry', 'admin', 'office',
    'sales', 'support', 'help', 'mail', 'email', 'general', 'reception',
    'accounts', 'billing', 'finance', 'hr', 'jobs', 'careers', 'recruitment',
    'marketing', 'press', 'media', 'news', 'team', 'staff', 'company',
    'business', 'service', 'services', 'customerservice', 'customerservices',
    'helpdesk', 'tech', 'technical', 'it', 'webmaster', 'postmaster',
    'noreply', 'no-reply', 'donotreply', 'auto', 'mailer', 'newsletter',
    'subscribe', 'unsubscribe', 'feedback', 'suggestions', 'complaints',
    'orders', 'order', 'booking', 'bookings', 'reservations', 'enquire',
    'query', 'queries', 'request', 'requests', 'inbox', 'main', 'central',
    'hq', 'headquarters', 'head', 'uk', 'london', 'england', 'britain',
    'privacy', 'legal', 'compliance', 'gdpr', 'data', 'security',
    'apply', 'applications', 'vacancies', 'work', 'employment',
]

def is_personal_email(email):
    """Check if email appears to be a personal email (not generic)"""
    if not email:
        return False
    
    prefix = email.split('@')[0].lower().strip()
    
    # Check against generic prefixes
    for generic in GENERIC_EMAIL_PREFIXES:
        if prefix == generic or prefix.startswith(generic + '.') or prefix.endswith('.' + generic):
            return False
    
    # If it contains a dot (like john.smith@) it's likely personal
    if '.' in prefix and len(prefix) > 3:
        return True
    
    # If it's short and not in generic list, might be initials (jsmith, js)
    # Be conservative - only count as personal if it looks like a name
    return False

# State
running = True
dry_run = False
use_hunter = True
max_limit = None
hunter_credits_exhausted = False
current_phase_index = 0

stats = {
    'processed': 0,
    'directors_found': 0,
    'websites_found': 0,
    'emails_found': 0,
    'phones_found': 0,
    'hunter_credits_used': 0,
    'errors': 0,
    'skipped': 0,
    'start_time': None,
    'phase_name': 'Accountants'
}


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    print("\n\n🛑 Stopping enrichment process...")
    running = False


def upload_to_gdrive():
    """Upload database to Google Drive using rclone or simple HTTP"""
    import subprocess
    import shutil
    import threading
    
    # Get file size for progress
    db_size = os.path.getsize(DB_PATH)
    db_size_mb = db_size / (1024 * 1024)
    
    print(f"\n☁️  Uploading database to Google Drive ({db_size_mb:.1f} MB)...")
    
    # Rclone path - check custom path first, then system PATH
    rclone_paths = [
        r'C:\Users\mrdea\rclone-v1.72.0-windows-amd64\rclone-v1.72.0-windows-amd64\rclone.exe',
        'rclone'  # fallback to system PATH
    ]
    
    rclone_cmd = None
    for path in rclone_paths:
        if os.path.exists(path) or shutil.which(path):
            rclone_cmd = path
            break
    
    # Method 1: Try rclone (if installed)
    if rclone_cmd:
        try:
            # Progress indicator while rclone runs
            upload_complete = threading.Event()
            
            def show_progress():
                spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
                idx = 0
                while not upload_complete.is_set():
                    print(f"\r   {spinner[idx]} Uploading {db_size_mb:.1f} MB...", end='', flush=True)
                    idx = (idx + 1) % len(spinner)
                    upload_complete.wait(0.1)
            
            progress_thread = threading.Thread(target=show_progress, daemon=True)
            progress_thread.start()
            
            result = subprocess.run([
                rclone_cmd, 'copy', 
                DB_PATH, 
                'gdrive:CompanyEnricher/',
                '--progress'
            ], capture_output=True, text=True)
            
            upload_complete.set()
            progress_thread.join(timeout=0.5)
            print("\r" + " " * 50 + "\r", end='')  # Clear the line
            
            if result.returncode == 0:
                print(f"✅ Database uploaded to Google Drive: CompanyEnricher/companies.db ({db_size_mb:.1f} MB)")
                return True
            else:
                print(f"⚠️ rclone error: {result.stderr}")
        except Exception as e:
            print(f"⚠️ rclone failed: {e}")
    
    # Method 2: Create timestamped copy for manual upload (with progress)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"companies_enriched_{timestamp}.db"
    backup_path = os.path.join(os.path.dirname(DB_PATH), backup_name)
    
    try:
        print(f"   📦 Creating backup copy...")
        
        # Copy with progress
        chunk_size = 1024 * 1024  # 1MB chunks
        copied = 0
        with open(DB_PATH, 'rb') as src, open(backup_path, 'wb') as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)
                progress = (copied / db_size) * 100
                bar_width = 30
                filled = int(bar_width * copied / db_size)
                bar = '█' * filled + '░' * (bar_width - filled)
                print(f"\r   [{bar}] {progress:.0f}%", end='', flush=True)
        
        print()  # New line after progress bar
        print(f"📁 Database backed up to: {backup_name}")
        print(f"   Upload this file manually to Google Drive")
        return True
    except Exception as e:
        print(f"\n❌ Backup failed: {e}")
        return False


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_pending_companies(limit=100):
    """Get companies that need enrichment - only not_attempted status"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get companies with favorite SICs that haven't been attempted yet
        placeholders = ','.join(['?' for _ in FAVORITE_SICS])
        cursor.execute(f'''
            SELECT c.id, c.company_number, c.company_name, c.sic_code_1,
                   c.directors_fetched, c.website_fetched, c.emails_fetched, c.phones_fetched,
                   c.website, c.enrichment_status
            FROM companies c
            WHERE c.company_status = 'Active'
            AND c.sic_code_1 IN ({placeholders})
            AND (c.enrichment_status = 'not_attempted' OR c.enrichment_status IS NULL)
            ORDER BY c.company_name ASC
            LIMIT ?
        ''', (*FAVORITE_SICS, limit))
        
        return [dict(row) for row in cursor.fetchall()]


def enrich_directors(company_number, company_id):
    """Fetch directors from Companies House API"""
    if not COMPANIES_HOUSE_API_KEY:
        return False, "No API key"
    
    try:
        response = requests.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}/officers",
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            directors_added = 0
            
            with get_db() as conn:
                cursor = conn.cursor()
                
                for item in data.get('items', []):
                    if item.get('resigned_on'):
                        continue
                    
                    name = item.get('name', '')
                    officer_role = item.get('officer_role', '')
                    appointed_on = item.get('appointed_on', '')
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO directors 
                        (company_id, company_number, name, officer_role, appointed_on)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (company_id, company_number, name, officer_role, appointed_on))
                    
                    if cursor.rowcount > 0:
                        directors_added += 1
                
                # Mark as fetched
                cursor.execute('''
                    UPDATE companies SET directors_fetched = 1, updated_at = ?
                    WHERE id = ?
                ''', (datetime.now().isoformat(), company_id))
                
                conn.commit()
            
            return True, directors_added
        else:
            return False, f"API error: {response.status_code}"
    
    except Exception as e:
        return False, str(e)


def find_website(company_name, company_number, company_id):
    """Try to find company website via DNS lookup"""
    import socket
    import re
    
    # Clean company name for domain guessing
    clean_name = re.sub(r'[^a-zA-Z0-9]', '', company_name.lower())
    clean_name = re.sub(r'(ltd|limited|plc|llp|uk|group|holdings|services|consulting)$', '', clean_name)
    
    # IDNA labels cannot exceed 63 characters - truncate to be safe
    if len(clean_name) > 60:
        clean_name = clean_name[:60]
    
    # Skip if name is too short after cleaning
    if len(clean_name) < 3:
        return False, "Name too short for domain guessing"
    
    # Domain patterns to try
    patterns = [
        f"{clean_name}.co.uk",
        f"{clean_name}.com",
        f"{clean_name}.uk",
    ]
    
    for domain in patterns:
        try:
            # Additional safety check for total domain length
            if len(domain) > 253:
                continue
            socket.gethostbyname(domain)
            # Domain exists, save it
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE companies SET website = ?, website_fetched = 1, updated_at = ?
                    WHERE id = ?
                ''', (domain, datetime.now().isoformat(), company_id))
                conn.commit()
            return True, domain
        except socket.gaierror:
            continue
        except UnicodeError:
            # IDNA encoding failed (e.g., label too long) - skip this pattern
            continue
    
    # Mark as attempted even if not found
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE companies SET website_fetched = 1, updated_at = ?
            WHERE id = ?
        ''', (datetime.now().isoformat(), company_id))
        conn.commit()
    
    return False, "No domain found"


def scrape_emails_and_phones(domain, company_number, company_id):
    """Scrape website for emails and phones. Returns (email_count, phone_count, email_list)"""
    from bs4 import BeautifulSoup
    import re
    
    if not domain:
        return 0, 0, []
    
    emails_found = 0
    phones_found = 0
    all_emails = set()
    
    # Pages to check - keep it focused for speed
    pages = ['', '/contact', '/about', '/contact-us', '/about-us', '/team']
    
    # Better browser headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Connection': 'close',
    }
    
    for page in pages:
        try:
            url = f"https://{domain}{page}"
            response = requests.get(url, timeout=10, headers=headers)
            
            if response.status_code != 200:
                continue
            
            text = response.text
            
            # Extract emails
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = set(re.findall(email_pattern, text))
            
            # Filter out common junk and image files (like logo@2x.png)
            junk_patterns = ['example', 'test', 'sample', 'wixpress', 'sentry', 'cloudflare', 'w3.org']
            image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.bmp', '.tiff']
            emails = [e.lower().strip() for e in emails 
                      if not any(x in e.lower() for x in junk_patterns)
                      and not any(e.lower().endswith(ext) for ext in image_extensions)
                      and '@' in e and not e.startswith('@')]
            
            all_emails.update(emails)
            
            # Extract UK phone numbers
            phone_pattern = r'(?:(?:\+44|0044|0)\s?[1-9]\d{1,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4})'
            phones = set(re.findall(phone_pattern, text))
            
            # Save to database
            with get_db() as conn:
                cursor = conn.cursor()
                
                for email in list(all_emails)[:5]:  # Limit to 5 emails
                    cursor.execute('''
                        INSERT OR IGNORE INTO emails 
                        (company_id, company_number, email, source, source_label)
                        VALUES (?, ?, ?, 'website', 'Website Scrape')
                    ''', (company_id, company_number, email))
                    if cursor.rowcount > 0:
                        emails_found += 1
                
                for phone in list(phones)[:2]:  # Limit to 2 phones
                    phone = re.sub(r'\s+', ' ', phone.strip())
                    cursor.execute('''
                        INSERT OR IGNORE INTO phones 
                        (company_id, company_number, phone, source)
                        VALUES (?, ?, ?, 'website')
                    ''', (company_id, company_number, phone))
                    if cursor.rowcount > 0:
                        phones_found += 1
                
                conn.commit()
            
            time.sleep(SCRAPE_DELAY)
            
        except Exception as e:
            continue
    
    # Mark as fetched
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE companies SET emails_fetched = 1, phones_fetched = 1, updated_at = ?
            WHERE id = ?
        ''', (datetime.now().isoformat(), company_id))
        conn.commit()
    
    return emails_found, phones_found, list(all_emails)


def hunter_email_search(domain, company_number, company_id):
    """Use Hunter.io domain search for emails. Returns (count, email_list, credits_exhausted)"""
    global hunter_credits_exhausted
    
    if not HUNTER_API_KEY or not domain:
        return 0, [], False
    
    try:
        response = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={'domain': domain, 'api_key': HUNTER_API_KEY},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            emails_found = 0
            found_emails = []
            
            email_list = data.get('data', {}).get('emails', [])
            
            if not email_list:
                return 0, [], False
            
            with get_db() as conn:
                cursor = conn.cursor()
                
                for email_data in email_list[:5]:
                    email = email_data.get('value', '').lower()
                    if not email:
                        continue
                    first_name = email_data.get('first_name', '')
                    last_name = email_data.get('last_name', '')
                    confidence = email_data.get('confidence', 0)
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO emails 
                        (company_id, company_number, email, source, source_label, 
                         first_name, last_name, confidence)
                        VALUES (?, ?, ?, 'hunter', 'Hunter.io', ?, ?, ?)
                    ''', (company_id, company_number, email, first_name, last_name, confidence))
                    
                    if cursor.rowcount > 0:
                        emails_found += 1
                        found_emails.append(email)
                
                conn.commit()
            
            return emails_found, found_emails, False
        
        elif response.status_code == 429:
            print(f"         ⚠️ Hunter rate limit! Waiting 60s...")
            time.sleep(60)
            return 0, [], False
        
        elif response.status_code == 402:
            # Credits exhausted
            print(f"\n         🚫 HUNTER CREDITS EXHAUSTED!")
            hunter_credits_exhausted = True
            return 0, [], True
        
        else:
            return 0, [], False
    
    except Exception as e:
        print(f"         ⚠️ Hunter error: {e}")
        return 0, [], False


def update_company_status(company_id, has_directors, has_website, has_emails, has_phones):
    """Update enrichment status based on what was found"""
    # Determine overall status
    if has_directors and has_emails:
        status = STATUS_SUCCESS
    elif has_directors or has_website or has_emails or has_phones:
        status = STATUS_PARTIAL
    else:
        status = STATUS_FAILED
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE companies 
            SET enrichment_status = ?, 
                last_enrichment_attempt = ?,
                updated_at = ?
            WHERE id = ?
        ''', (status, datetime.now().isoformat(), datetime.now().isoformat(), company_id))
        conn.commit()
    
    return status


def print_stats(remaining=None):
    """Print current statistics"""
    elapsed = time.time() - stats['start_time'] if stats['start_time'] else 0
    rate = stats['processed'] / elapsed * 3600 if elapsed > 0 else 1000
    
    # Calculate ETA
    eta_str = ""
    if remaining and rate > 0:
        hours_remaining = remaining / rate
        if hours_remaining < 1:
            eta_str = f"~{int(hours_remaining * 60)} min"
        elif hours_remaining < 24:
            eta_str = f"~{hours_remaining:.1f} hrs"
        else:
            eta_str = f"~{hours_remaining/24:.1f} days"
    
    print(f"\n{'─'*50}")
    print(f"📊 PROGRESS REPORT")
    print(f"{'─'*50}")
    print(f"   Companies processed: {stats['processed']:,}")
    print(f"   Skipped (already done): {stats['skipped']:,}")
    print(f"   Directors found:     {stats['directors_found']:,}")
    print(f"   Websites found:      {stats['websites_found']:,}")
    print(f"   Emails found:        {stats['emails_found']:,}")
    print(f"   Phones found:        {stats['phones_found']:,}")
    print(f"   Hunter credits used: {stats['hunter_credits_used']:,}")
    print(f"   Errors:              {stats['errors']:,}")
    print(f"{'─'*50}")
    print(f"   Rate:                {rate:,.0f} companies/hour")
    print(f"   Elapsed:             {elapsed/60:.1f} minutes")
    if remaining:
        print(f"   Remaining:           {remaining:,} companies")
        print(f"   ETA:                 {eta_str}")
    print(f"{'─'*50}")


def run_phase(phase_name, sic_codes, to_process):
    """Run enrichment for a single phase. Returns True if completed, False if interrupted."""
    global running, max_limit, hunter_credits_exhausted, FAVORITE_SICS
    
    FAVORITE_SICS = sic_codes  # Update global for get_pending_companies
    stats['phase_name'] = phase_name
    total_to_process = to_process
    
    while running and not hunter_credits_exhausted:
        # Check if we've hit the limit
        if max_limit and stats['processed'] >= max_limit:
            print(f"\n✅ Reached limit of {max_limit} companies")
            break
        
        # Get batch of companies to process
        batch_size = min(50, max_limit - stats['processed']) if max_limit else 50
        companies = get_pending_companies(limit=batch_size)
        
        if not companies:
            print("\n" + "="*60)
            print("✅ ALL FAVORITE COMPANIES ENRICHED!")
            print("="*60)
            break
        
        if total_to_process == 0:
            # First batch - count total
            with get_db() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in FAVORITE_SICS])
                cursor.execute(f'''
                    SELECT COUNT(*) FROM companies 
                    WHERE company_status = 'Active' 
                    AND sic_code_1 IN ({placeholders})
                    AND (enrichment_status = 'not_attempted' OR enrichment_status IS NULL)
                ''', tuple(FAVORITE_SICS))
                total_to_process = cursor.fetchone()[0]
                
                # Also show already processed
                cursor.execute(f'''
                    SELECT enrichment_status, COUNT(*) FROM companies 
                    WHERE company_status = 'Active' AND sic_code_1 IN ({placeholders})
                    GROUP BY enrichment_status
                ''', tuple(FAVORITE_SICS))
                status_counts = dict(cursor.fetchall())
            
            print(f"📋 Target: {total_to_process:,} accountants/tax companies to enrich")
            print(f"   Already done: {status_counts.get('success', 0):,} success, {status_counts.get('partial', 0):,} partial, {status_counts.get('failed', 0):,} failed")
            print()
        
        for company in companies:
            if not running:
                break
            
            if max_limit and stats['processed'] >= max_limit:
                break
            
            company_id = company['id']
            company_number = company['company_number']
            company_name = company['company_name']
            
            # Progress indicator
            progress = f"[{stats['processed']+1:,}/{total_to_process:,}]"
            print(f"\n{progress} 🔄 {company_name}")
            print(f"         Company #: {company_number}")
            
            if dry_run:
                print("         [DRY RUN - skipping]")
                stats['processed'] += 1
                continue
            
            has_directors = False
            has_website = False
            has_emails = False
            has_phones = False
            
            # 1. Enrich Directors
            if not company['directors_fetched']:
                success, result = enrich_directors(company_number, company_id)
                if success and result > 0:
                    has_directors = True
                    stats['directors_found'] += result
                    print(f"         ✓ Directors: {result} found")
                elif success:
                    print(f"         ○ Directors: none found")
                else:
                    print(f"         ✗ Directors: {result}")
                    stats['errors'] += 1
                time.sleep(CH_DELAY)
            else:
                has_directors = True  # Already fetched previously
            
            # 2. Find Website
            website = company['website']
            if not company['website_fetched']:
                success, result = find_website(company_name, company_number, company_id)
                if success:
                    website = result
                    has_website = True
                    stats['websites_found'] += 1
                    print(f"         ✓ Website: {result}")
                else:
                    print(f"         ○ Website: not found")
            elif website:
                has_website = True
            
            # 3. Scrape Emails & Phones
            scraped_emails_list = []
            if website and not company['emails_fetched']:
                emails, phones, scraped_emails_list = scrape_emails_and_phones(website, company_number, company_id)
                if emails > 0:
                    has_emails = True
                if phones > 0:
                    has_phones = True
                stats['emails_found'] += emails
                stats['phones_found'] += phones
                print(f"         ✓ Scraped: {emails} emails, {phones} phones")
                for em in scraped_emails_list[:3]:  # Show first 3
                    personal = "👤" if is_personal_email(em) else "📧"
                    print(f"           {personal} {em}")
                if len(scraped_emails_list) > 3:
                    print(f"           ... and {len(scraped_emails_list) - 3} more")
            
            # 4. Hunter Email Enrichment - check if we have PERSONAL emails
            if website and use_hunter and HUNTER_API_KEY and not hunter_credits_exhausted:
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT email FROM emails WHERE company_number = ?', 
                                   (company_number,))
                    existing_emails = [row[0] for row in cursor.fetchall()]
                
                # Count personal vs generic emails
                personal_count = sum(1 for e in existing_emails if is_personal_email(e))
                generic_count = len(existing_emails) - personal_count
                
                if personal_count >= 2:
                    print(f"         ○ Hunter: skipped ({personal_count} personal emails found)")
                elif len(existing_emails) >= 2 and personal_count == 0:
                    print(f"         🔍 Trying Hunter (have {generic_count} generic emails, need personal)...")
                    hunter_found, hunter_emails, credits_out = hunter_email_search(website, company_number, company_id)
                    if credits_out:
                        print(f"         🚫 Hunter credits exhausted - stopping to upload...")
                        running = False
                    elif hunter_found > 0:
                        has_emails = True
                        stats['emails_found'] += hunter_found
                        stats['hunter_credits_used'] += 1
                        print(f"         ✓ Hunter: {hunter_found} emails (1 credit)")
                        for em in hunter_emails[:3]:
                            print(f"           👤 {em}")
                    else:
                        print(f"         ○ Hunter: no emails found")
                    time.sleep(HUNTER_DELAY)
                elif len(existing_emails) < 2:
                    print(f"         🔍 Trying Hunter for {website}...")
                    hunter_found, hunter_emails, credits_out = hunter_email_search(website, company_number, company_id)
                    if credits_out:
                        print(f"         🚫 Hunter credits exhausted - stopping to upload...")
                        running = False
                    elif hunter_found > 0:
                        has_emails = True
                        stats['emails_found'] += hunter_found
                        stats['hunter_credits_used'] += 1
                        print(f"         ✓ Hunter: {hunter_found} emails (1 credit)")
                        for em in hunter_emails[:3]:
                            print(f"           👤 {em}")
                    else:
                        print(f"         ○ Hunter: no emails found")
                    time.sleep(HUNTER_DELAY)
                else:
                    print(f"         ○ Hunter: skipped ({personal_count} personal + {generic_count} generic)")
            
            # Update enrichment status
            status = update_company_status(company_id, has_directors, has_website, has_emails, has_phones)
            print(f"         → Status: {status.upper()}")
            
            stats['processed'] += 1
            
            # Print summary every 25 companies
            if stats['processed'] % 25 == 0:
                remaining = to_process - stats['processed']
                print_stats(remaining)
        
        # Small pause between batches
        if running and not hunter_credits_exhausted:
            time.sleep(0.5)
    
    # Return whether phase completed successfully
    return not hunter_credits_exhausted and running


def get_next_company_to_process():
    """Get the name of the next company that will be processed (for resume info)"""
    with get_db() as conn:
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in FAVORITE_SICS])
        cursor.execute(f'''
            SELECT company_name FROM companies
            WHERE company_status = 'Active'
            AND sic_code_1 IN ({placeholders})
            AND (enrichment_status = 'not_attempted' OR enrichment_status IS NULL)
            ORDER BY company_name ASC
            LIMIT 1
        ''', tuple(FAVORITE_SICS))
        result = cursor.fetchone()
        return result[0] if result else None


def run_enrichment():
    """Main enrichment loop - processes all SIC phases in order"""
    global running, hunter_credits_exhausted, current_phase_index, FAVORITE_SICS
    
    stats['start_time'] = time.time()
    
    print("\n" + "="*60)
    print("🚀 BACKGROUND ENRICHER - MULTI-PHASE MODE")
    print("="*60)
    print(f"   Mode:        {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"   Hunter.io:   {'ENABLED' if use_hunter and HUNTER_API_KEY else 'DISABLED'}")
    print(f"   Limit:       {max_limit if max_limit else 'Unlimited'}")
    print(f"   Database:    {DB_PATH}")
    print(f"   CH API:      {'✅ Configured' if COMPANIES_HOUSE_API_KEY else '❌ Missing'}")
    print(f"   Hunter API:  {'✅ Configured' if HUNTER_API_KEY else '❌ Missing'}")
    print("="*60)
    print("   📋 ENRICHMENT PHASES (in order):")
    for i, phase in enumerate(SIC_PHASES):
        print(f"      Phase {i+1}: {phase['name']} ({', '.join(phase['codes'])})")
    
    # Show resume point
    next_company = get_next_company_to_process()
    if next_company:
        print("="*60)
        print(f"   ▶️  RESUMING FROM: {next_company}")
    print("="*60)
    print("   Press Ctrl+C to stop gracefully")
    print("="*60 + "\n")
    
    # Process each phase
    for phase_index, phase in enumerate(SIC_PHASES):
        if not running:
            break
        if hunter_credits_exhausted:
            print(f"\n🚫 Hunter credits exhausted - stopping before Phase {phase_index + 1}")
            break
        
        current_phase_index = phase_index
        phase_name = phase['name']
        sic_codes = phase['codes']
        FAVORITE_SICS = sic_codes
        
        # Get phase stats
        with get_db() as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?' for _ in sic_codes])
            
            cursor.execute(f'''
                SELECT COUNT(*) FROM companies 
                WHERE company_status = 'Active' AND sic_code_1 IN ({placeholders})
            ''', tuple(sic_codes))
            total_in_phase = cursor.fetchone()[0]
            
            cursor.execute(f'''
                SELECT enrichment_status, COUNT(*) FROM companies 
                WHERE company_status = 'Active' AND sic_code_1 IN ({placeholders})
                GROUP BY enrichment_status
            ''', tuple(sic_codes))
            status_breakdown = dict(cursor.fetchall())
            
            cursor.execute(f'''
                SELECT COUNT(*) FROM companies 
                WHERE company_status = 'Active' AND sic_code_1 IN ({placeholders})
                AND (enrichment_status = 'not_attempted' OR enrichment_status IS NULL)
            ''', tuple(sic_codes))
            to_process = cursor.fetchone()[0]
        
        # Skip if nothing to process
        if to_process == 0:
            print(f"\n✅ Phase {phase_index + 1}: {phase_name} - Already complete!")
            continue
        
        # Print phase header
        estimated_hours = to_process / 1000
        print("\n" + "="*60)
        print(f"📦 PHASE {phase_index + 1}: {phase_name.upper()}")
        print("="*60)
        print(f"   SIC Codes:   {', '.join(sic_codes)}")
        print(f"   Total in phase:             {total_in_phase:,}")
        print(f"   ├─ Success:                 {status_breakdown.get('success', 0):,}")
        print(f"   ├─ Partial:                 {status_breakdown.get('partial', 0):,}")
        print(f"   ├─ Failed:                  {status_breakdown.get('failed', 0):,}")
        print(f"   └─ To process:              {to_process:,}")
        if estimated_hours < 1:
            print(f"   Estimated time:             ~{int(estimated_hours * 60)} minutes")
        elif estimated_hours < 24:
            print(f"   Estimated time:             ~{estimated_hours:.1f} hours")
        else:
            print(f"   Estimated time:             ~{estimated_hours/24:.1f} days")
        print("="*60 + "\n")
        
        # Run phase
        phase_completed = run_phase(phase_name, sic_codes, to_process)
        
        # Print phase stats
        print_stats(to_process - stats['processed'])
        
        if phase_completed:
            print(f"\n✅ Phase {phase_index + 1}: {phase_name} COMPLETE!")
        else:
            if hunter_credits_exhausted:
                print(f"\n🚫 Phase {phase_index + 1}: {phase_name} - Stopped (Hunter credits exhausted)")
            else:
                print(f"\n⏹️ Phase {phase_index + 1}: {phase_name} - Stopped by user")
        
        # Upload after each phase completion or credit exhaustion
        if os.getenv('GDRIVE_UPLOAD') == 'true':
            print(f"\n☁️  Uploading after Phase {phase_index + 1}...")
            upload_to_gdrive()
        
        # Reset stats for next phase
        if phase_completed and phase_index < len(SIC_PHASES) - 1:
            stats['processed'] = 0
            stats['directors_found'] = 0
            stats['websites_found'] = 0
            stats['emails_found'] = 0
            stats['phones_found'] = 0
            stats['hunter_credits_used'] = 0
            stats['errors'] = 0
    
    print("\n" + "="*60)
    print("🏁 ALL ENRICHMENT PHASES FINISHED!")
    print("="*60 + "\n")


def main():
    global dry_run, use_hunter, max_limit
    
    parser = argparse.ArgumentParser(
        description='Background enricher for favorite companies',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python background_enricher.py                    # Run full enrichment
  python background_enricher.py --limit 100        # Process only 100 companies
  python background_enricher.py --no-hunter        # Skip Hunter.io (free only)
  python background_enricher.py --dry-run          # Preview without changes
  
To stop: Press Ctrl+C (will finish current company gracefully)
        '''
    )
    parser.add_argument('--limit', type=int, help='Max companies to process')
    parser.add_argument('--no-hunter', action='store_true', help='Skip Hunter.io (free enrichment only)')
    parser.add_argument('--dry-run', action='store_true', help='Preview mode - no database changes')
    
    args = parser.parse_args()
    
    dry_run = args.dry_run
    use_hunter = not args.no_hunter
    max_limit = args.limit
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        run_enrichment()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

