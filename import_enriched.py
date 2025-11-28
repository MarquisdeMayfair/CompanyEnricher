#!/usr/bin/env python3
"""
Import Enriched CSV Data into Database

This script imports previously enriched data (directors, emails, websites)
from your existing CSV exports back into the SQLite database.

Usage:
    python import_enriched.py                           # Import all enriched_*.csv files
    python import_enriched.py enriched_accountants.csv  # Import specific file
"""

import csv
import os
import sys
import glob
from database import get_db, add_director, add_email, update_company_website

def parse_director_name(name_str):
    """Parse director name from 'LASTNAME, Firstname' format"""
    if not name_str or not name_str.strip():
        return None
    
    name = name_str.strip()
    if ',' in name:
        parts = name.split(',')
        last_name = parts[0].strip()
        first_name = parts[1].strip().split()[0] if len(parts) > 1 else ''
        return {
            'name': name,
            'first_name': first_name,
            'last_name': last_name,
            'role': 'director'
        }
    else:
        parts = name.split()
        return {
            'name': name,
            'first_name': parts[0] if parts else '',
            'last_name': parts[-1] if len(parts) > 1 else '',
            'role': 'director'
        }


def import_enriched_csv(csv_path):
    """Import enriched data from a single CSV file"""
    print(f"\n📄 Importing: {os.path.basename(csv_path)}")
    
    companies_updated = 0
    directors_added = 0
    emails_added = 0
    websites_added = 0
    not_found = 0
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                company_number = row.get('Company Number', '').strip()
                if not company_number:
                    continue
                
                # Find company in database
                cursor.execute('''
                    SELECT id, website, enrichment_status 
                    FROM companies WHERE company_number = ?
                ''', (company_number,))
                company = cursor.fetchone()
                
                if not company:
                    not_found += 1
                    continue
                
                company_id = company['id']
                updated = False
                
                # Import website if we have one and database doesn't
                website = row.get('Website', '').strip()
                website_source = row.get('Website Source', '').strip()
                if website and not company['website']:
                    cursor.execute('''
                        UPDATE companies SET website = ?, website_source = ?, website_fetched = 1
                        WHERE id = ?
                    ''', (website, website_source or 'imported', company_id))
                    websites_added += 1
                    updated = True
                
                # Import directors
                for i in range(1, 4):  # Director 1, 2, 3
                    director_name = row.get(f'Director {i}', '').strip()
                    if director_name:
                        director = parse_director_name(director_name)
                        if director:
                            cursor.execute('''
                                INSERT OR IGNORE INTO directors 
                                (company_id, company_number, name, first_name, last_name, officer_role)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                company_id, company_number,
                                director['name'], director['first_name'], 
                                director['last_name'], director['role']
                            ))
                            if cursor.rowcount > 0:
                                directors_added += 1
                                updated = True
                
                # Import emails (up to 5)
                for i in range(1, 6):
                    email = row.get(f'Email {i}', '').strip()
                    if email and '@' in email:
                        source = row.get(f'Email {i} Source', 'imported').strip()
                        verified = row.get(f'Email {i} Verified', '').strip()
                        score = row.get(f'Email {i} Score', '').strip()
                        
                        # Determine verification status
                        is_verified = 0
                        verification_status = None
                        verification_score = None
                        
                        if verified and verified.lower() not in ['not verified', '']:
                            is_verified = 1
                            if 'valid' in verified.lower():
                                verification_status = 'valid'
                            elif 'invalid' in verified.lower():
                                verification_status = 'invalid'
                            elif 'accept' in verified.lower() or 'risky' in verified.lower():
                                verification_status = 'accept_all'
                        
                        if score and score.isdigit():
                            verification_score = int(score)
                        
                        # Map source labels back to source codes
                        source_code = source.lower()
                        if 'hunter' in source_code:
                            source_code = 'hunter'
                        elif 'website' in source_code:
                            source_code = 'website_scrape'
                        elif 'import' in source_code:
                            source_code = 'imported'
                        else:
                            source_code = 'imported'
                        
                        cursor.execute('''
                            INSERT OR IGNORE INTO emails 
                            (company_id, company_number, email, source, source_label, 
                             verified, verification_status, verification_score)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            company_id, company_number, email.lower(),
                            source_code, source,
                            is_verified, verification_status, verification_score
                        ))
                        if cursor.rowcount > 0:
                            emails_added += 1
                            updated = True
                
                # Update enrichment status if we added data
                if updated:
                    cursor.execute('''
                        UPDATE companies SET 
                            enrichment_status = 'success',
                            directors_fetched = CASE WHEN EXISTS(
                                SELECT 1 FROM directors WHERE company_id = ?
                            ) THEN 1 ELSE directors_fetched END,
                            emails_fetched = CASE WHEN EXISTS(
                                SELECT 1 FROM emails WHERE company_id = ?
                            ) THEN 1 ELSE emails_fetched END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (company_id, company_id, company_id))
                    companies_updated += 1
            
            conn.commit()
    
    print(f"   ✅ Companies updated: {companies_updated}")
    print(f"   👤 Directors added: {directors_added}")
    print(f"   📧 Emails added: {emails_added}")
    print(f"   🌐 Websites added: {websites_added}")
    if not_found > 0:
        print(f"   ⚠️  Not found in DB: {not_found}")
    
    return {
        'companies': companies_updated,
        'directors': directors_added,
        'emails': emails_added,
        'websites': websites_added,
        'not_found': not_found
    }


def main():
    print("📊 Enriched Data Import Tool")
    print("=" * 50)
    
    # Get list of files to import
    if len(sys.argv) > 1:
        # Specific file provided
        files = [sys.argv[1]]
    else:
        # Find all enriched CSV files
        base_path = os.path.dirname(os.path.abspath(__file__))
        files = glob.glob(os.path.join(base_path, 'enriched_*.csv'))
        files += glob.glob(os.path.join(base_path, 'clean_emails_*.csv'))
        files += glob.glob(os.path.join(base_path, 'ACSP*.csv'))
    
    if not files:
        print("❌ No enriched CSV files found")
        return
    
    print(f"Found {len(files)} file(s) to import")
    
    # Track totals
    totals = {
        'companies': 0,
        'directors': 0,
        'emails': 0,
        'websites': 0,
        'not_found': 0
    }
    
    for file_path in sorted(files):
        if os.path.exists(file_path):
            result = import_enriched_csv(file_path)
            for key in totals:
                totals[key] += result.get(key, 0)
    
    print("\n" + "=" * 50)
    print("📈 TOTAL IMPORT SUMMARY")
    print("=" * 50)
    print(f"✅ Companies updated: {totals['companies']}")
    print(f"👤 Directors added: {totals['directors']}")
    print(f"📧 Emails added: {totals['emails']}")
    print(f"🌐 Websites added: {totals['websites']}")
    print(f"⚠️  Not found in DB: {totals['not_found']}")


if __name__ == '__main__':
    main()

