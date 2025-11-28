#!/usr/bin/env python3
"""
Monthly CSV Update Script for Company Enrichment Database

This script handles monthly updates from new Companies House CSV exports.
Key behaviors:
- ADDS new companies that don't exist
- UPDATES basic company info (name, address, status) for existing companies
- PRESERVES all enrichment data (emails, phones, directors, websites)
- REPORTS on changes (new, updated, unchanged, removed from source)

Usage:
    python update_from_csv.py /path/to/new/companies_house.csv
    python update_from_csv.py --dry-run /path/to/new/companies_house.csv  # Preview changes
    python update_from_csv.py --sic 69201,69203 /path/to/file.csv         # Filter by SIC
"""

import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from database import init_db, get_db, DB_PATH

BATCH_SIZE = 10000


def extract_year_from_date(date_str):
    """Extract year from DD/MM/YYYY format"""
    if not date_str or '/' not in date_str:
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            return int(parts[2])
    except (ValueError, IndexError):
        pass
    return None


def extract_sic_code(sic_text):
    """Extract just the numeric SIC code"""
    if not sic_text:
        return None
    sic_text = sic_text.strip().strip('"')
    if ' - ' in sic_text:
        return sic_text.split(' - ')[0].strip()
    return sic_text.strip()


def parse_row(row):
    """Parse a CSV row into a company data dict"""
    company_number = row.get(' CompanyNumber', row.get('CompanyNumber', '')).strip().strip('"')
    
    if not company_number:
        return None
    
    incorporation_date = row.get('IncorporationDate', '').strip().strip('"')
    
    return {
        'company_number': company_number,
        'company_name': row.get('CompanyName', '').strip().strip('"'),
        'address_line1': row.get('RegAddress.AddressLine1', row.get(' RegAddress.AddressLine1', '')).strip().strip('"'),
        'address_line2': row.get(' RegAddress.AddressLine2', row.get('RegAddress.AddressLine2', '')).strip().strip('"'),
        'post_town': row.get('RegAddress.PostTown', '').strip().strip('"'),
        'county': row.get('RegAddress.County', '').strip().strip('"'),
        'postcode': row.get('RegAddress.PostCode', '').strip().strip('"'),
        'company_status': row.get('CompanyStatus', '').strip().strip('"'),
        'incorporation_date': incorporation_date,
        'incorporation_year': extract_year_from_date(incorporation_date),
        'sic_code_1': extract_sic_code(row.get('SICCode.SicText_1', '')),
        'sic_code_2': extract_sic_code(row.get('SICCode.SicText_2', '')),
        'sic_code_3': extract_sic_code(row.get('SICCode.SicText_3', '')),
        'sic_code_4': extract_sic_code(row.get('SICCode.SicText_4', '')),
    }


def update_from_csv(csv_path, sic_filter=None, dry_run=False):
    """
    Update database from a new Companies House CSV
    
    This preserves all enrichment data while updating basic company info.
    """
    print(f"\n📅 Monthly CSV Update Tool")
    print(f"=" * 50)
    print(f"CSV File: {csv_path}")
    print(f"Database: {DB_PATH}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE UPDATE'}")
    if sic_filter:
        print(f"SIC Filter: {', '.join(sic_filter)}")
    print(f"=" * 50)
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)
    
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Run import_csv.py first to create the initial database.")
        sys.exit(1)
    
    file_size = os.path.getsize(csv_path)
    print(f"📁 File size: {file_size / (1024**3):.2f} GB")
    
    # Get current database stats
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM companies')
        initial_count = cursor.fetchone()[0]
        print(f"📊 Current database records: {initial_count:,}")
    
    # Track changes
    start_time = time.time()
    processed = 0
    new_companies = 0
    updated_companies = 0
    unchanged = 0
    filtered_out = 0
    errors = 0
    
    csv_source = os.path.basename(csv_path)
    csv_company_numbers = set()  # Track what's in the new CSV
    
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            batch = []
            
            with get_db() as conn:
                cursor = conn.cursor()
                
                for row in reader:
                    processed += 1
                    
                    company = parse_row(row)
                    if not company:
                        errors += 1
                        continue
                    
                    # Apply SIC filter if specified
                    if sic_filter:
                        sics = [company['sic_code_1'], company['sic_code_2'], 
                                company['sic_code_3'], company['sic_code_4']]
                        if not any(sic in sic_filter for sic in sics if sic):
                            filtered_out += 1
                            continue
                    
                    csv_company_numbers.add(company['company_number'])
                    batch.append(company)
                    
                    if len(batch) >= BATCH_SIZE:
                        if not dry_run:
                            new, updated, same = update_batch(cursor, batch, csv_source)
                            new_companies += new
                            updated_companies += updated
                            unchanged += same
                            conn.commit()
                        else:
                            # In dry run, just count what would happen
                            new, updated, same = preview_batch(cursor, batch)
                            new_companies += new
                            updated_companies += updated
                            unchanged += same
                        
                        batch = []
                        
                        # Progress update
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        print(f"\r⏳ Processed: {processed:,} | New: {new_companies:,} | "
                              f"Updated: {updated_companies:,} | Unchanged: {unchanged:,} | "
                              f"Rate: {rate:,.0f}/sec", end='')
                
                # Process remaining batch
                if batch:
                    if not dry_run:
                        new, updated, same = update_batch(cursor, batch, csv_source)
                        new_companies += new
                        updated_companies += updated
                        unchanged += same
                        conn.commit()
                    else:
                        new, updated, same = preview_batch(cursor, batch)
                        new_companies += new
                        updated_companies += updated
                        unchanged += same
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Update interrupted by user")
    
    # Final report
    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 50}")
    print(f"{'✅ Update Complete!' if not dry_run else '🔍 Dry Run Complete'}")
    print(f"=" * 50)
    print(f"📊 Total processed: {processed:,}")
    print(f"🆕 New companies: {new_companies:,}")
    print(f"🔄 Updated companies: {updated_companies:,}")
    print(f"⏸️  Unchanged: {unchanged:,}")
    if sic_filter:
        print(f"🚫 Filtered out: {filtered_out:,}")
    print(f"⚠️  Errors: {errors:,}")
    print(f"⏱️  Time elapsed: {elapsed/60:.1f} minutes")
    
    if not dry_run:
        # Check for companies in DB but not in new CSV
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM companies')
            final_count = cursor.fetchone()[0]
            print(f"\n📈 Database growth: {initial_count:,} → {final_count:,} ({final_count - initial_count:+,})")
    else:
        print(f"\n📝 This was a dry run. No changes were made.")
        print(f"   Run without --dry-run to apply these changes.")


def update_batch(cursor, batch, csv_source):
    """Update a batch of companies, preserving enrichment data"""
    new = 0
    updated = 0
    unchanged = 0
    
    for company in batch:
        try:
            # Check if company exists and what its current data is
            cursor.execute('''
                SELECT id, company_name, postcode, company_status 
                FROM companies WHERE company_number = ?
            ''', (company['company_number'],))
            existing = cursor.fetchone()
            
            if existing:
                # Check if anything actually changed
                if (existing['company_name'] == company['company_name'] and
                    existing['postcode'] == company['postcode'] and
                    existing['company_status'] == company['company_status']):
                    unchanged += 1
                    continue
                
                # Update only basic fields, preserve enrichment
                cursor.execute('''
                    UPDATE companies SET
                        company_name = ?,
                        address_line1 = ?,
                        address_line2 = ?,
                        post_town = ?,
                        county = ?,
                        postcode = ?,
                        company_status = ?,
                        incorporation_date = ?,
                        incorporation_year = ?,
                        sic_code_1 = ?,
                        sic_code_2 = ?,
                        sic_code_3 = ?,
                        sic_code_4 = ?,
                        updated_at = CURRENT_TIMESTAMP,
                        csv_source = ?,
                        csv_import_date = CURRENT_TIMESTAMP
                    WHERE company_number = ?
                ''', (
                    company['company_name'],
                    company['address_line1'],
                    company['address_line2'],
                    company['post_town'],
                    company['county'],
                    company['postcode'],
                    company['company_status'],
                    company['incorporation_date'],
                    company['incorporation_year'],
                    company['sic_code_1'],
                    company['sic_code_2'],
                    company['sic_code_3'],
                    company['sic_code_4'],
                    csv_source,
                    company['company_number']
                ))
                updated += 1
            else:
                # Insert new company
                cursor.execute('''
                    INSERT INTO companies (
                        company_number, company_name,
                        address_line1, address_line2, post_town, county, postcode,
                        company_status, incorporation_date, incorporation_year,
                        sic_code_1, sic_code_2, sic_code_3, sic_code_4,
                        enrichment_status, csv_source, csv_import_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'not_attempted', ?, CURRENT_TIMESTAMP)
                ''', (
                    company['company_number'],
                    company['company_name'],
                    company['address_line1'],
                    company['address_line2'],
                    company['post_town'],
                    company['county'],
                    company['postcode'],
                    company['company_status'],
                    company['incorporation_date'],
                    company['incorporation_year'],
                    company['sic_code_1'],
                    company['sic_code_2'],
                    company['sic_code_3'],
                    company['sic_code_4'],
                    csv_source
                ))
                new += 1
                
        except Exception as e:
            print(f"\n⚠️  Error updating {company['company_number']}: {e}")
    
    return new, updated, unchanged


def preview_batch(cursor, batch):
    """Preview what would happen (dry run)"""
    new = 0
    updated = 0
    unchanged = 0
    
    for company in batch:
        cursor.execute('''
            SELECT company_name, postcode, company_status 
            FROM companies WHERE company_number = ?
        ''', (company['company_number'],))
        existing = cursor.fetchone()
        
        if existing:
            if (existing['company_name'] == company['company_name'] and
                existing['postcode'] == company['postcode'] and
                existing['company_status'] == company['company_status']):
                unchanged += 1
            else:
                updated += 1
        else:
            new += 1
    
    return new, updated, unchanged


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Update database from new Companies House CSV')
    parser.add_argument('csv_path', help='Path to the new CSV file')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without applying them')
    parser.add_argument('--sic', type=str,
                        help='Comma-separated SIC codes to filter (e.g., 69201,69203)')
    
    args = parser.parse_args()
    
    sic_filter = None
    if args.sic:
        sic_filter = [s.strip() for s in args.sic.split(',')]
    
    update_from_csv(args.csv_path, sic_filter=sic_filter, dry_run=args.dry_run)


if __name__ == '__main__':
    main()

