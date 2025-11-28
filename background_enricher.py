#!/usr/bin/env python3
"""
Background Enricher for Favorite Companies
==========================================
Automatically enriches companies with favorite SIC codes.

Process:
1. Directors (Companies House API) 
2. Website (free DNS lookup)
3. Emails & Phones (free web scraping)
4. Email enrichment (Hunter.io - paid, only if needed)

Usage:
    Start:  cd "/Users/nik/Downloads/DATA LISTS" && source venv/bin/activate && python background_enricher.py
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

# Favorite SIC codes
FAVORITE_SICS = ['82990', '69201', '69203', '82110', '70229']

# Rate limiting (seconds between requests)
CH_DELAY = 1.0      # Companies House: 1 req/sec
HUNTER_DELAY = 3.0  # Hunter: 1 req/3sec (conservative)
SCRAPE_DELAY = 0.5  # Web scraping: 2 req/sec

# Enrichment status values
STATUS_NOT_ATTEMPTED = 'not_attempted'
STATUS_PARTIAL = 'partial'
STATUS_SUCCESS = 'success'
STATUS_FAILED = 'failed'

# State
running = True
dry_run = False
use_hunter = True
max_limit = None

stats = {
    'processed': 0,
    'directors_found': 0,
    'websites_found': 0,
    'emails_found': 0,
    'phones_found': 0,
    'hunter_credits_used': 0,
    'errors': 0,
    'skipped': 0,
    'start_time': None
}


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    print("\n\n🛑 Stopping enrichment process...")
    running = False


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_pending_companies(limit=100):
    """Get companies that need enrichment"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get companies with favorite SICs that haven't been fully enriched
        placeholders = ','.join(['?' for _ in FAVORITE_SICS])
        cursor.execute(f'''
            SELECT c.id, c.company_number, c.company_name, c.sic_code_1,
                   c.directors_fetched, c.website_fetched, c.emails_fetched, c.phones_fetched,
                   c.website
            FROM companies c
            WHERE c.company_status = 'Active'
            AND c.sic_code_1 IN ({placeholders})
            AND (c.directors_fetched = 0 OR c.website_fetched = 0 
                 OR c.emails_fetched = 0 OR c.phones_fetched = 0)
            ORDER BY c.directors_fetched ASC, c.website_fetched ASC
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
    
    # Domain patterns to try
    patterns = [
        f"{clean_name}.co.uk",
        f"{clean_name}.com",
        f"{clean_name}.uk",
    ]
    
    for domain in patterns:
        try:
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
    """Scrape website for emails and phones"""
    from bs4 import BeautifulSoup
    import re
    
    if not domain:
        return 0, 0
    
    emails_found = 0
    phones_found = 0
    
    # Pages to check
    pages = ['', '/contact', '/about', '/contact-us', '/about-us']
    
    for page in pages:
        try:
            url = f"https://{domain}{page}"
            response = requests.get(url, timeout=5, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; CompanyEnricher/1.0)'
            })
            
            if response.status_code != 200:
                continue
            
            text = response.text
            
            # Extract emails
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = set(re.findall(email_pattern, text))
            
            # Filter out common junk
            emails = [e for e in emails if not any(x in e.lower() for x in 
                ['example', 'test', 'sample', 'wixpress', 'sentry'])]
            
            # Extract UK phone numbers
            phone_pattern = r'(?:(?:\+44|0044|0)\s?[1-9]\d{1,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4})'
            phones = set(re.findall(phone_pattern, text))
            
            # Save to database
            with get_db() as conn:
                cursor = conn.cursor()
                
                for email in emails[:5]:  # Limit to 5 emails
                    email = email.lower().strip()
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
    
    return emails_found, phones_found


def hunter_email_search(domain, company_number, company_id):
    """Use Hunter.io domain search for emails"""
    if not HUNTER_API_KEY or not domain:
        return 0
    
    try:
        response = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={'domain': domain, 'api_key': HUNTER_API_KEY},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            emails_found = 0
            
            with get_db() as conn:
                cursor = conn.cursor()
                
                for email_data in data.get('data', {}).get('emails', [])[:5]:
                    email = email_data.get('value', '').lower()
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
                
                conn.commit()
            
            return emails_found
        
        return 0
    
    except Exception as e:
        return 0


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


def print_stats():
    """Print current statistics"""
    elapsed = time.time() - stats['start_time'] if stats['start_time'] else 0
    rate = stats['processed'] / elapsed * 3600 if elapsed > 0 else 0
    
    print(f"\n{'─'*50}")
    print(f"📊 PROGRESS REPORT")
    print(f"{'─'*50}")
    print(f"   Companies processed: {stats['processed']}")
    print(f"   Skipped (already done): {stats['skipped']}")
    print(f"   Directors found:     {stats['directors_found']}")
    print(f"   Websites found:      {stats['websites_found']}")
    print(f"   Emails found:        {stats['emails_found']}")
    print(f"   Phones found:        {stats['phones_found']}")
    print(f"   Hunter credits used: {stats['hunter_credits_used']}")
    print(f"   Errors:              {stats['errors']}")
    print(f"   Rate:                {rate:.1f} companies/hour")
    print(f"   Elapsed:             {elapsed/60:.1f} minutes")
    print(f"{'─'*50}")


def run_enrichment():
    """Main enrichment loop"""
    global running, max_limit
    
    stats['start_time'] = time.time()
    
    print("\n" + "="*60)
    print("🚀 BACKGROUND ENRICHER STARTED")
    print("="*60)
    print(f"   Mode:        {'DRY RUN (no changes)' if dry_run else 'LIVE'}")
    print(f"   Hunter.io:   {'ENABLED' if use_hunter and HUNTER_API_KEY else 'DISABLED'}")
    print(f"   Limit:       {max_limit if max_limit else 'Unlimited'}")
    print(f"   Favorite SICs: {', '.join(FAVORITE_SICS)}")
    print(f"   Database:    {DB_PATH}")
    print(f"   CH API:      {'✅ Configured' if COMPANIES_HOUSE_API_KEY else '❌ Missing'}")
    print(f"   Hunter API:  {'✅ Configured' if HUNTER_API_KEY else '❌ Missing'}")
    print("="*60)
    print("   Press Ctrl+C to stop gracefully")
    print("="*60 + "\n")
    
    total_to_process = 0
    
    while running:
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
            print(f"📋 Found {total_to_process:,} companies needing enrichment\n")
        
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
            if website and not company['emails_fetched']:
                emails, phones = scrape_emails_and_phones(website, company_number, company_id)
                if emails > 0:
                    has_emails = True
                if phones > 0:
                    has_phones = True
                stats['emails_found'] += emails
                stats['phones_found'] += phones
                print(f"         ✓ Scraped: {emails} emails, {phones} phones")
            
            # 4. Hunter Email Enrichment
            if website and use_hunter and HUNTER_API_KEY:
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM emails WHERE company_number = ?', 
                                   (company_number,))
                    email_count = cursor.fetchone()[0]
                
                if email_count < 2:
                    hunter_found = hunter_email_search(website, company_number, company_id)
                    if hunter_found > 0:
                        has_emails = True
                        stats['emails_found'] += hunter_found
                        stats['hunter_credits_used'] += 1
                        print(f"         ✓ Hunter: {hunter_found} emails (1 credit)")
                    time.sleep(HUNTER_DELAY)
            
            # Update enrichment status
            status = update_company_status(company_id, has_directors, has_website, has_emails, has_phones)
            print(f"         → Status: {status.upper()}")
            
            stats['processed'] += 1
            
            # Print summary every 25 companies
            if stats['processed'] % 25 == 0:
                print_stats()
        
        # Small pause between batches
        if running:
            time.sleep(0.5)
    
    print_stats()
    print("\n🏁 Enrichment process finished!\n")


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

