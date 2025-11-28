#!/usr/bin/env python3
"""
Import Companies House CSV into SQLite Database

This script handles the initial import of the large Companies House CSV file (~2.6GB, ~5.6M records)
into the SQLite database. It's designed for efficiency with:
- Batch inserts (10,000 records at a time)
- Progress reporting
- Memory-efficient streaming (no pandas)
- Resume capability (skips existing records)

Usage:
    python import_csv.py                                    # Uses default CSV path
    python import_csv.py /path/to/companies.csv            # Custom CSV path
    python import_csv.py --resume                          # Resume interrupted import
    python import_csv.py --sic 69201,69203,82990,82110,70229  # Import only specific SIC codes
"""

import csv
import os
import sys
import time
import sqlite3
from datetime import datetime
from pathlib import Path

# Import our database module
from database import init_db, get_db, DB_PATH

# Default CSV path
DEFAULT_CSV = 'BasicCompanyDataAsOneFile-2025-11-01.csv'
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
    """Extract just the numeric SIC code from 'CODE - Description' format"""
    if not sic_text:
        return None
    sic_text = sic_text.strip().strip('"')
    if ' - ' in sic_text:
        return sic_text.split(' - ')[0].strip()
    return sic_text.strip()


def parse_row(row):
    """Parse a CSV row into a company data dict"""
    # Handle the weird column name with leading space
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


def import_csv(csv_path, sic_filter=None, resume=False):
    """
    Import companies from CSV into database
    
    Args:
        csv_path: Path to the Companies House CSV file
        sic_filter: Optional list of SIC codes to filter by (e.g., ['69201', '69203'])
        resume: If True, skip existing records (for resuming interrupted import)
    """
    print(f"\n📊 Company Data Import Tool")
    print(f"=" * 50)
    print(f"CSV File: {csv_path}")
    print(f"Database: {DB_PATH}")
    print(f"Batch Size: {BATCH_SIZE:,}")
    if sic_filter:
        print(f"SIC Filter: {', '.join(sic_filter)}")
    if resume:
        print(f"Resume Mode: ON (skipping existing records)")
    print(f"=" * 50)
    
    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)
    
    # Get file size for progress
    file_size = os.path.getsize(csv_path)
    print(f"📁 File size: {file_size / (1024**3):.2f} GB")
    
    # Initialize database
    init_db()
    
    # Track progress
    start_time = time.time()
    processed = 0
    inserted = 0
    updated = 0
    skipped = 0
    filtered_out = 0
    errors = 0
    
    # Prepare batch insert
    batch = []
    csv_source = os.path.basename(csv_path)
    
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            with get_db() as conn:
                cursor = conn.cursor()
                
                for row in reader:
                    processed += 1
                    
                    # Parse the row
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
                    
                    # Add to batch
                    batch.append(company)
                    
                    # Process batch when full
                    if len(batch) >= BATCH_SIZE:
                        batch_inserted, batch_updated, batch_skipped = insert_batch(
                            cursor, batch, csv_source, resume
                        )
                        inserted += batch_inserted
                        updated += batch_updated
                        skipped += batch_skipped
                        conn.commit()
                        batch = []
                        
                        # Progress update
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        # Estimate ~5.6M records total for Companies House CSV
                        estimated_total = 5600000
                        remaining = estimated_total - processed
                        eta = remaining / rate if rate > 0 else 0
                        
                        print(f"\r⏳ Processed: {processed:,} | Inserted: {inserted:,} | "
                              f"Updated: {updated:,} | Skipped: {skipped:,} | "
                              f"Rate: {rate:,.0f}/sec | ETA: {eta/60:.1f}min", end='')
                
                # Insert remaining batch
                if batch:
                    batch_inserted, batch_updated, batch_skipped = insert_batch(
                        cursor, batch, csv_source, resume
                    )
                    inserted += batch_inserted
                    updated += batch_updated
                    skipped += batch_skipped
                    conn.commit()
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Import interrupted by user")
        print(f"   Progress saved. Use --resume to continue.")
    
    # Final stats
    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 50}")
    print(f"✅ Import Complete!")
    print(f"=" * 50)
    print(f"📊 Total processed: {processed:,}")
    print(f"➕ Inserted: {inserted:,}")
    print(f"🔄 Updated: {updated:,}")
    print(f"⏭️  Skipped (existing): {skipped:,}")
    if sic_filter:
        print(f"🚫 Filtered out: {filtered_out:,}")
    print(f"⚠️  Errors: {errors:,}")
    print(f"⏱️  Time elapsed: {elapsed/60:.1f} minutes")
    print(f"💾 Database size: {os.path.getsize(DB_PATH) / (1024**2):.1f} MB")


def insert_batch(cursor, batch, csv_source, resume=False):
    """Insert a batch of companies using INSERT OR IGNORE / UPDATE"""
    inserted = 0
    updated = 0
    skipped = 0
    
    for company in batch:
        try:
            if resume:
                # Check if exists
                cursor.execute('SELECT id FROM companies WHERE company_number = ?', 
                               (company['company_number'],))
                if cursor.fetchone():
                    skipped += 1
                    continue
            
            # Try insert first
            cursor.execute('''
                INSERT OR IGNORE INTO companies (
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
            
            if cursor.rowcount > 0:
                inserted += 1
            else:
                # Record exists, update basic fields (preserve enrichment data)
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
                
        except sqlite3.Error as e:
            print(f"\n⚠️  Error inserting {company['company_number']}: {e}")
    
    return inserted, updated, skipped


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Companies House CSV into database')
    parser.add_argument('csv_path', nargs='?', default=DEFAULT_CSV,
                        help=f'Path to CSV file (default: {DEFAULT_CSV})')
    parser.add_argument('--resume', action='store_true',
                        help='Resume interrupted import (skip existing records)')
    parser.add_argument('--sic', type=str,
                        help='Comma-separated SIC codes to filter (e.g., 69201,69203)')
    
    args = parser.parse_args()
    
    sic_filter = None
    if args.sic:
        sic_filter = [s.strip() for s in args.sic.split(',')]
    
    import_csv(args.csv_path, sic_filter=sic_filter, resume=args.resume)


if __name__ == '__main__':
    main()

