"""
SQLite Database Module for Company Enrichment Tool

Schema Design:
- companies: All 5.6M companies from Companies House CSV
- directors: Officers/directors linked to companies
- emails: Email addresses with source, verification status
- phones: Phone numbers with source
- enrichment_log: Track what we've attempted to avoid re-processing

Enrichment Status Values:
- NULL/not_attempted: Never tried enriching
- success: Found data
- failed: Attempted but no data found
- partial: Some data found (e.g., website but no email)

Override Logic:
- Default search returns: not_attempted records first
- Override toggle allows: re-processing failed records
- Never auto-re-process: success records (manual re-enrich only)
"""

import sqlite3
import os
from datetime import datetime
from contextlib import contextmanager

DB_PATH = os.getenv('DB_PATH', 'companies.db')


@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return dicts instead of tuples
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the database with all tables"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Companies table - main entity
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_number TEXT UNIQUE NOT NULL,
                company_name TEXT NOT NULL,
                
                -- Address fields
                address_line1 TEXT,
                address_line2 TEXT,
                post_town TEXT,
                county TEXT,
                postcode TEXT,
                
                -- Company info
                company_status TEXT,
                incorporation_date TEXT,
                incorporation_year INTEGER,  -- Extracted for fast filtering
                
                -- SIC codes (up to 4 per company)
                sic_code_1 TEXT,
                sic_code_2 TEXT,
                sic_code_3 TEXT,
                sic_code_4 TEXT,
                
                -- Enriched data
                website TEXT,
                website_source TEXT,  -- 'inferred', 'hunter', 'imported'
                main_phone TEXT,
                phone_source TEXT,  -- 'website', 'hunter', 'imported'
                
                -- Enrichment tracking
                enrichment_status TEXT DEFAULT 'not_attempted',
                -- Status: not_attempted, directors_only, website_only, partial, success, failed
                directors_fetched INTEGER DEFAULT 0,  -- Boolean
                website_fetched INTEGER DEFAULT 0,
                emails_fetched INTEGER DEFAULT 0,
                phones_fetched INTEGER DEFAULT 0,
                
                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_enrichment_attempt TIMESTAMP,
                
                -- Source tracking
                csv_source TEXT,  -- Which CSV file this came from
                csv_import_date TIMESTAMP
            )
        ''')
        
        # Indexes for fast searching
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_number ON companies(company_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_name ON companies(company_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_postcode ON companies(postcode)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_incorporation_year ON companies(incorporation_year)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_status ON companies(company_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrichment_status ON companies(enrichment_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sic_code_1 ON companies(sic_code_1)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sic_code_2 ON companies(sic_code_2)')
        
        # Directors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS directors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                company_number TEXT NOT NULL,
                
                name TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                officer_role TEXT,
                appointed_on TEXT,
                resigned_on TEXT,  -- NULL if still active
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_directors_company ON directors(company_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_directors_company_number ON directors(company_number)')
        
        # Emails table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                company_number TEXT NOT NULL,
                director_id INTEGER,  -- NULL if general company email
                
                email TEXT NOT NULL,
                source TEXT NOT NULL,  -- 'website_scrape', 'hunter', 'imported'
                source_label TEXT,
                match_type TEXT,  -- 'company', 'auditor', 'agent', 'other'
                confidence INTEGER,
                
                -- Verification
                verified INTEGER DEFAULT 0,
                verification_status TEXT,  -- 'valid', 'invalid', 'accept_all', 'webmail', 'unknown'
                verification_score INTEGER,
                verified_at TIMESTAMP,
                
                -- Names from Hunter
                first_name TEXT,
                last_name TEXT,
                position TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                FOREIGN KEY (director_id) REFERENCES directors(id) ON DELETE SET NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_emails_company ON emails(company_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_emails_company_number ON emails(company_number)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_emails_unique ON emails(company_number, email)')
        
        # Phones table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS phones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                company_number TEXT NOT NULL,
                
                phone TEXT NOT NULL,
                phone_type TEXT DEFAULT 'main',  -- 'main', 'fax', 'mobile', 'other'
                source TEXT NOT NULL,  -- 'website', 'hunter', 'imported'
                source_url TEXT,  -- Where we found it
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phones_company ON phones(company_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_phones_company_number ON phones(company_number)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_phones_unique ON phones(company_number, phone)')
        
        # Enrichment log - for tracking what we've tried
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                company_number TEXT NOT NULL,
                
                action TEXT NOT NULL,  -- 'fetch_directors', 'find_website', 'scrape_emails', 'hunter_emails', 'scrape_phones', 'hunter_phones', 'verify_emails'
                status TEXT NOT NULL,  -- 'success', 'failed', 'partial', 'skipped'
                details TEXT,  -- JSON with any additional info
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrichment_log_company ON enrichment_log(company_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_enrichment_log_action ON enrichment_log(action)')
        
        conn.commit()
        print("✅ Database initialized successfully")


def get_company_by_number(company_number):
    """Get a single company by company number"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.*, 
                   GROUP_CONCAT(DISTINCT d.name) as director_names,
                   GROUP_CONCAT(DISTINCT e.email) as email_list,
                   GROUP_CONCAT(DISTINCT p.phone) as phone_list
            FROM companies c
            LEFT JOIN directors d ON c.id = d.company_id AND d.resigned_on IS NULL
            LEFT JOIN emails e ON c.id = e.company_id
            LEFT JOIN phones p ON c.id = p.company_id
            WHERE c.company_number = ?
            GROUP BY c.id
        ''', (company_number,))
        return cursor.fetchone()


def search_companies(
    sic_codes=None,
    postcode_prefix=None,
    year_filter=None,
    status_filter='Active',
    enrichment_filter='not_attempted',  # 'not_attempted', 'failed', 'all'
    include_enriched=False,  # Override to include already enriched
    limit=500,
    offset=0
):
    """
    Search companies with filters
    
    Default behavior: Returns unattempted records first
    Override (include_enriched=True): Also returns failed records for retry
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        conditions = []
        params = []
        
        # Status filter (Active by default)
        if status_filter:
            conditions.append("c.company_status = ?")
            params.append(status_filter)
        
        # SIC code filter
        if sic_codes:
            if isinstance(sic_codes, str):
                sic_codes = [sic_codes]
            sic_conditions = []
            for sic in sic_codes:
                sic_conditions.append("(c.sic_code_1 LIKE ? OR c.sic_code_2 LIKE ? OR c.sic_code_3 LIKE ? OR c.sic_code_4 LIKE ?)")
                params.extend([f"{sic}%"] * 4)
            conditions.append(f"({' OR '.join(sic_conditions)})")
        
        # Postcode filter
        if postcode_prefix:
            conditions.append("c.postcode LIKE ?")
            params.append(f"{postcode_prefix.upper()}%")
        
        # Year filter
        if year_filter:
            if year_filter == 'pre2022':
                conditions.append("c.incorporation_year < 2022")
            else:
                conditions.append("c.incorporation_year = ?")
                params.append(int(year_filter))
        
        # Enrichment filter - KEY LOGIC
        if not include_enriched:
            if enrichment_filter == 'not_attempted':
                conditions.append("(c.enrichment_status = 'not_attempted' OR c.enrichment_status IS NULL)")
            elif enrichment_filter == 'failed':
                conditions.append("c.enrichment_status = 'failed'")
            elif enrichment_filter == 'retry':
                # Both not_attempted and failed
                conditions.append("(c.enrichment_status IN ('not_attempted', 'failed') OR c.enrichment_status IS NULL)")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        # Build query with LEFT JOINs for related data
        query = f'''
            SELECT c.*,
                   COUNT(DISTINCT d.id) as director_count,
                   COUNT(DISTINCT e.id) as email_count,
                   COUNT(DISTINCT p.id) as phone_count
            FROM companies c
            LEFT JOIN directors d ON c.id = d.company_id AND d.resigned_on IS NULL
            LEFT JOIN emails e ON c.id = e.company_id
            LEFT JOIN phones p ON c.id = p.company_id
            {where_clause}
            GROUP BY c.id
            ORDER BY c.enrichment_status ASC, c.company_name ASC
            LIMIT ? OFFSET ?
        '''
        
        params.extend([limit, offset])
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            company = dict(row)
            
            # Get directors for this company
            cursor.execute('''
                SELECT name, officer_role, appointed_on 
                FROM directors 
                WHERE company_id = ? AND resigned_on IS NULL
                ORDER BY appointed_on DESC
            ''', (company['id'],))
            company['directors'] = [dict(d) for d in cursor.fetchall()]
            
            # Get emails
            cursor.execute('''
                SELECT email, source, source_label, match_type, confidence,
                       verified, verification_status, verification_score,
                       first_name, last_name, position
                FROM emails 
                WHERE company_id = ?
                ORDER BY verification_status = 'valid' DESC, confidence DESC
            ''', (company['id'],))
            company['emails'] = [dict(e) for e in cursor.fetchall()]
            
            # Get phones
            cursor.execute('''
                SELECT phone, phone_type, source
                FROM phones 
                WHERE company_id = ?
            ''', (company['id'],))
            company['phones'] = [dict(p) for p in cursor.fetchall()]
            
            results.append(company)
        
        return results


def count_companies(sic_codes=None, postcode_prefix=None, year_filter=None, enrichment_filter='all'):
    """Get count of companies matching filters"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        conditions = ["c.company_status = 'Active'"]
        params = []
        
        if sic_codes:
            if isinstance(sic_codes, str):
                sic_codes = [sic_codes]
            sic_conditions = []
            for sic in sic_codes:
                sic_conditions.append("(c.sic_code_1 LIKE ? OR c.sic_code_2 LIKE ?)")
                params.extend([f"{sic}%"] * 2)
            conditions.append(f"({' OR '.join(sic_conditions)})")
        
        if postcode_prefix:
            conditions.append("c.postcode LIKE ?")
            params.append(f"{postcode_prefix.upper()}%")
        
        if year_filter:
            if year_filter == 'pre2022':
                conditions.append("c.incorporation_year < 2022")
            else:
                conditions.append("c.incorporation_year = ?")
                params.append(int(year_filter))
        
        if enrichment_filter == 'not_attempted':
            conditions.append("(c.enrichment_status = 'not_attempted' OR c.enrichment_status IS NULL)")
        elif enrichment_filter == 'failed':
            conditions.append("c.enrichment_status = 'failed'")
        
        where_clause = "WHERE " + " AND ".join(conditions)
        
        cursor.execute(f'''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) as enriched,
                   SUM(CASE WHEN enrichment_status = 'failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN enrichment_status = 'not_attempted' OR enrichment_status IS NULL THEN 1 ELSE 0 END) as unattempted
            FROM companies c
            {where_clause}
        ''', params)
        
        return dict(cursor.fetchone())


def upsert_company(company_data, csv_source=None):
    """
    Insert or update a company
    For monthly CSV updates: only updates non-enriched fields, preserves enrichment data
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Check if company exists
        cursor.execute('SELECT id, enrichment_status FROM companies WHERE company_number = ?', 
                       (company_data['company_number'],))
        existing = cursor.fetchone()
        
        if existing:
            # Update only basic fields, preserve enrichment data
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
                company_data.get('company_name'),
                company_data.get('address_line1'),
                company_data.get('address_line2'),
                company_data.get('post_town'),
                company_data.get('county'),
                company_data.get('postcode'),
                company_data.get('company_status'),
                company_data.get('incorporation_date'),
                company_data.get('incorporation_year'),
                company_data.get('sic_code_1'),
                company_data.get('sic_code_2'),
                company_data.get('sic_code_3'),
                company_data.get('sic_code_4'),
                csv_source,
                company_data['company_number']
            ))
            return existing['id']
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
                company_data['company_number'],
                company_data.get('company_name'),
                company_data.get('address_line1'),
                company_data.get('address_line2'),
                company_data.get('post_town'),
                company_data.get('county'),
                company_data.get('postcode'),
                company_data.get('company_status'),
                company_data.get('incorporation_date'),
                company_data.get('incorporation_year'),
                company_data.get('sic_code_1'),
                company_data.get('sic_code_2'),
                company_data.get('sic_code_3'),
                company_data.get('sic_code_4'),
                csv_source
            ))
            conn.commit()
            return cursor.lastrowid


def add_director(company_id, company_number, director_data):
    """Add a director to a company"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Parse name into first/last
        name = director_data.get('name', '')
        if ',' in name:
            parts = name.split(',')
            last_name = parts[0].strip()
            first_name = parts[1].strip().split()[0] if len(parts) > 1 else ''
        else:
            parts = name.split()
            first_name = parts[0] if parts else ''
            last_name = parts[-1] if len(parts) > 1 else ''
        
        cursor.execute('''
            INSERT OR IGNORE INTO directors (
                company_id, company_number, name, first_name, last_name,
                officer_role, appointed_on, resigned_on
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            company_id,
            company_number,
            name,
            first_name,
            last_name,
            director_data.get('role', director_data.get('officer_role')),
            director_data.get('appointed', director_data.get('appointed_on')),
            director_data.get('resigned_on')
        ))
        conn.commit()
        return cursor.lastrowid


def add_email(company_id, company_number, email_data, director_id=None):
    """Add an email to a company (deduplicates automatically)"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO emails (
                    company_id, company_number, director_id,
                    email, source, source_label, match_type, confidence,
                    verified, verification_status, verification_score,
                    first_name, last_name, position
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                company_id,
                company_number,
                director_id,
                email_data.get('email', '').lower().strip(),
                email_data.get('source'),
                email_data.get('source_label'),
                email_data.get('match_type'),
                email_data.get('confidence'),
                email_data.get('verified', 0),
                email_data.get('verification_status'),
                email_data.get('verification_score'),
                email_data.get('first_name'),
                email_data.get('last_name'),
                email_data.get('position')
            ))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Email already exists for this company
            return None


def add_phone(company_id, company_number, phone_data):
    """Add a phone number to a company (deduplicates automatically)"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO phones (
                    company_id, company_number,
                    phone, phone_type, source, source_url
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                company_id,
                company_number,
                phone_data.get('phone', '').strip(),
                phone_data.get('phone_type', 'main'),
                phone_data.get('source'),
                phone_data.get('source_url')
            ))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Phone already exists for this company
            return None


def update_enrichment_status(company_number, status, action=None, details=None):
    """Update enrichment status and log the action"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get company ID
        cursor.execute('SELECT id FROM companies WHERE company_number = ?', (company_number,))
        row = cursor.fetchone()
        if not row:
            return False
        
        company_id = row['id']
        
        # Determine what fields to mark as fetched
        updates = {
            'enrichment_status': status,
            'last_enrichment_attempt': datetime.now().isoformat()
        }
        
        if action == 'fetch_directors':
            updates['directors_fetched'] = 1
        elif action in ('find_website', 'scrape_emails'):
            updates['website_fetched'] = 1
        elif action in ('scrape_emails', 'hunter_emails'):
            updates['emails_fetched'] = 1
        elif action in ('scrape_phones', 'hunter_phones'):
            updates['phones_fetched'] = 1
        
        # Build update query
        set_clause = ', '.join(f"{k} = ?" for k in updates.keys())
        cursor.execute(f'''
            UPDATE companies SET {set_clause} WHERE company_number = ?
        ''', list(updates.values()) + [company_number])
        
        # Log the action
        if action:
            cursor.execute('''
                INSERT INTO enrichment_log (company_id, company_number, action, status, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (company_id, company_number, action, status, details))
        
        conn.commit()
        return True


def update_email_verification(email, verification_result):
    """Update email verification status"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE emails SET
                verified = 1,
                verification_status = ?,
                verification_score = ?,
                verified_at = CURRENT_TIMESTAMP
            WHERE email = ?
        ''', (
            verification_result.get('status'),
            verification_result.get('score'),
            email
        ))
        conn.commit()
        return cursor.rowcount > 0


def update_company_website(company_number, website, source):
    """Update company website"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE companies SET
                website = ?,
                website_source = ?,
                website_fetched = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE company_number = ?
        ''', (website, source, company_number))
        conn.commit()
        return cursor.rowcount > 0


def update_company_phone(company_number, phone, source):
    """Update main company phone"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE companies SET
                main_phone = ?,
                phone_source = ?,
                phones_fetched = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE company_number = ?
        ''', (phone, source, company_number))
        conn.commit()
        return cursor.rowcount > 0


def get_db_stats():
    """Get database statistics"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        stats = {}
        
        # Total companies
        cursor.execute('SELECT COUNT(*) FROM companies')
        stats['total_companies'] = cursor.fetchone()[0]
        
        # Active companies
        cursor.execute("SELECT COUNT(*) FROM companies WHERE company_status = 'Active'")
        stats['active_companies'] = cursor.fetchone()[0]
        
        # Enrichment status breakdown
        cursor.execute('''
            SELECT enrichment_status, COUNT(*) as count
            FROM companies
            GROUP BY enrichment_status
        ''')
        stats['enrichment_breakdown'] = dict(cursor.fetchall())
        
        # Directors count
        cursor.execute('SELECT COUNT(*) FROM directors WHERE resigned_on IS NULL')
        stats['total_directors'] = cursor.fetchone()[0]
        
        # Emails count
        cursor.execute('SELECT COUNT(*) FROM emails')
        stats['total_emails'] = cursor.fetchone()[0]
        
        # Verified emails
        cursor.execute("SELECT COUNT(*) FROM emails WHERE verified = 1")
        stats['verified_emails'] = cursor.fetchone()[0]
        
        # Valid emails
        cursor.execute("SELECT COUNT(*) FROM emails WHERE verification_status = 'valid'")
        stats['valid_emails'] = cursor.fetchone()[0]
        
        # Phones count
        cursor.execute('SELECT COUNT(*) FROM phones')
        stats['total_phones'] = cursor.fetchone()[0]
        
        # Companies with websites
        cursor.execute("SELECT COUNT(*) FROM companies WHERE website IS NOT NULL AND website != ''")
        stats['companies_with_website'] = cursor.fetchone()[0]
        
        return stats


if __name__ == '__main__':
    print("Initializing database...")
    init_db()
    print("\nDatabase stats:")
    stats = get_db_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

