import os
import csv
import requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
import time

load_dotenv()

app = Flask(__name__, static_folder='static')
CORS(app)

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
COMPANIES_HOUSE_API_KEY = os.getenv('COMPANIES_HOUSE_API_KEY')
CSV_PATH = os.getenv('CSV_PATH', 'BasicCompanyDataAsOneFile-2025-11-01.csv')

# SIC Code mappings
SIC_CODES = {
    'accountants': ['69201', '69203'],
    'management_consultancy': ['70229'],
    'business_support': ['82990', '82110'],
    'all_target': ['82990', '69201', '69203', '82110', '70229']
}

SIC_DESCRIPTIONS = {
    '82990': 'Other business support service activities',
    '69201': 'Accounting and auditing activities',
    '69203': 'Tax consultancy',
    '82110': 'Combined office administrative service activities',
    '70229': 'Management consultancy activities'
}


def get_officers(company_number):
    """Fetch officers/directors from Companies House API"""
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
    try:
        response = requests.get(
            url,
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            directors = []
            for officer in data.get('items', []):
                if officer.get('officer_role') in ['director', 'corporate-director']:
                    if officer.get('resigned_on') is None:  # Only active directors
                        directors.append({
                            'name': officer.get('name', ''),
                            'role': officer.get('officer_role', ''),
                            'appointed': officer.get('appointed_on', '')
                        })
            return directors
        elif response.status_code == 429:
            return {'error': 'rate_limited'}
        else:
            return []
    except Exception as e:
        print(f"Error fetching officers for {company_number}: {e}")
        return []


def get_email_from_hunter(domain):
    """Find email addresses using Hunter.io Domain Search"""
    if not domain or not HUNTER_API_KEY:
        return []
    
    # Clean the domain
    domain = domain.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
    
    url = f"https://api.hunter.io/v2/domain-search"
    try:
        response = requests.get(
            url,
            params={'domain': domain, 'api_key': HUNTER_API_KEY},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            emails = []
            for email in data.get('data', {}).get('emails', [])[:3]:
                emails.append({
                    'email': email.get('value', ''),
                    'first_name': email.get('first_name', ''),
                    'last_name': email.get('last_name', ''),
                    'position': email.get('position', ''),
                    'confidence': email.get('confidence', 0)
                })
            return emails
        return []
    except Exception as e:
        print(f"Error fetching emails for {domain}: {e}")
        return []


def find_email_for_person(first_name, last_name, company_name):
    """Find email for a specific person using Hunter.io Email Finder"""
    if not HUNTER_API_KEY or not first_name or not last_name:
        return None
    
    # Try to derive a domain from company name
    # Clean company name to create potential domain
    domain = company_name.lower()
    # Remove common suffixes
    for suffix in [' limited', ' ltd', ' llp', ' plc', ' inc', ' corporation', ' corp', ' & co', ' and co']:
        domain = domain.replace(suffix, '')
    # Clean and format as domain
    domain = domain.strip().replace(' ', '').replace(',', '').replace('.', '')
    domain = f"{domain}.co.uk"  # UK companies typically use .co.uk
    
    url = "https://api.hunter.io/v2/email-finder"
    try:
        response = requests.get(
            url,
            params={
                'domain': domain,
                'first_name': first_name,
                'last_name': last_name,
                'api_key': HUNTER_API_KEY
            },
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            email_data = data.get('data', {})
            if email_data.get('email'):
                return {
                    'email': email_data.get('email', ''),
                    'confidence': email_data.get('score', 0),
                    'domain': domain
                }
        # Try .com as fallback
        domain_com = domain.replace('.co.uk', '.com')
        response = requests.get(
            url,
            params={
                'domain': domain_com,
                'first_name': first_name,
                'last_name': last_name,
                'api_key': HUNTER_API_KEY
            },
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            email_data = data.get('data', {})
            if email_data.get('email'):
                return {
                    'email': email_data.get('email', ''),
                    'confidence': email_data.get('score', 0),
                    'domain': domain_com
                }
        return None
    except Exception as e:
        print(f"Error finding email for {first_name} {last_name}: {e}")
        return None


def search_company_domain(company_name):
    """Search for company domain using Hunter.io Discover"""
    if not HUNTER_API_KEY or not company_name:
        return None
    
    url = "https://api.hunter.io/v2/domain-search"
    
    # Try common domain patterns
    clean_name = company_name.lower()
    for suffix in [' limited', ' ltd', ' llp', ' plc', ' inc', ' corporation', ' corp', ' & co', ' and co']:
        clean_name = clean_name.replace(suffix, '')
    clean_name = clean_name.strip().replace(' ', '').replace(',', '').replace('.', '')
    
    domains_to_try = [
        f"{clean_name}.co.uk",
        f"{clean_name}.com",
        f"{clean_name}.uk"
    ]
    
    for domain in domains_to_try:
        try:
            response = requests.get(
                url,
                params={'domain': domain, 'api_key': HUNTER_API_KEY},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('data', {}).get('emails'):
                    return {
                        'domain': domain,
                        'emails': data['data']['emails'][:5],
                        'pattern': data['data'].get('pattern', '')
                    }
        except:
            continue
    
    return None


def find_company_domain(company_name):
    """Find company domain using Hunter.io Company Enrichment API"""
    if not HUNTER_API_KEY or not company_name:
        return None
    
    # Clean company name for search
    clean_name = company_name
    for suffix in [' LIMITED', ' LTD', ' LLP', ' PLC', ' INC', ' CORPORATION', ' CORP']:
        clean_name = clean_name.replace(suffix, '')
    clean_name = clean_name.strip()
    
    # Try Hunter.io Domain Search with company name
    url = "https://api.hunter.io/v2/domain-search"
    try:
        # First, try to find via company name pattern matching
        # Generate potential domains
        name_slug = clean_name.lower()
        for char in [' ', ',', '.', '&', "'", '-']:
            name_slug = name_slug.replace(char, '')
        
        potential_domains = [
            f"{name_slug}.co.uk",
            f"{name_slug}.com",
            f"{name_slug}.uk",
            f"{name_slug}accountants.co.uk",
            f"{name_slug}-accountants.co.uk",
        ]
        
        for domain in potential_domains:
            try:
                response = requests.get(
                    url,
                    params={'domain': domain, 'api_key': HUNTER_API_KEY},
                    timeout=5
                )
                if response.status_code == 200:
                    data = response.json()
                    # Check if domain exists and has data
                    if data.get('data', {}).get('domain'):
                        return {
                            'domain': data['data']['domain'],
                            'organization': data['data'].get('organization', ''),
                            'pattern': data['data'].get('pattern', ''),
                            'emails_count': len(data['data'].get('emails', []))
                        }
            except:
                continue
        
        return None
        
    except Exception as e:
        print(f"Error finding domain for {company_name}: {e}")
        return None


def get_company_profile(company_number):
    """Fetch company profile from Companies House API to get any available web links"""
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    try:
        response = requests.get(
            url,
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return {
                'company_name': data.get('company_name', ''),
                'company_status': data.get('company_status', ''),
                'type': data.get('type', ''),
                'sic_codes': data.get('sic_codes', []),
                'links': data.get('links', {}),
                'external_registration_number': data.get('external_registration_number', ''),
                'registered_office_address': data.get('registered_office_address', {})
            }
        return None
    except Exception as e:
        print(f"Error fetching company profile {company_number}: {e}")
        return None


def get_company_filing_description(company_number):
    """Check company filings for website mentions - FREE via Companies House"""
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/filing-history"
    try:
        response = requests.get(
            url,
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            params={'items_per_page': 10},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        return []
    except Exception as e:
        print(f"Error fetching filings for {company_number}: {e}")
        return []




def extract_emails_from_text(text):
    """Extract email addresses from text using regex"""
    import re
    if not text:
        return []
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = list(set(re.findall(email_pattern, text.lower())))
    # Filter out common false positives
    filtered = []
    for email in emails:
        # Skip image files, CSS, etc.
        if any(ext in email for ext in ['.png', '.jpg', '.gif', '.css', '.js', '.svg']):
            continue
        # Skip example emails
        if 'example' in email or 'test@' in email or 'email@' in email:
            continue
        filtered.append(email)
    return filtered


def scrape_website_for_emails(domain):
    """Scrape a company website for email addresses - COMPLETELY FREE"""
    if not domain:
        return []
    
    emails_found = []
    pages_to_try = [
        f"https://{domain}",
        f"https://{domain}/contact",
        f"https://{domain}/contact-us",
        f"https://{domain}/about",
        f"https://{domain}/about-us",
        f"https://www.{domain}",
        f"https://www.{domain}/contact",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for url in pages_to_try[:4]:  # Limit to 4 pages to be respectful
        try:
            response = requests.get(url, headers=headers, timeout=5, allow_redirects=True)
            if response.status_code == 200:
                # Extract emails from HTML
                page_emails = extract_emails_from_text(response.text)
                for email in page_emails:
                    # Check if email domain matches the website domain
                    email_domain = email.split('@')[-1]
                    if domain in email_domain or email_domain in domain:
                        if email not in [e['email'] for e in emails_found]:
                            emails_found.append({
                                'email': email,
                                'source': 'website_scrape',
                                'source_label': 'Website',
                                'url': url,
                                'confidence': 85
                            })
                
                # Also check for mailto: links
                import re
                mailto_pattern = r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
                mailto_emails = re.findall(mailto_pattern, response.text.lower())
                for email in mailto_emails:
                    email_domain = email.split('@')[-1]
                    if domain in email_domain or email_domain in domain:
                        if email not in [e['email'] for e in emails_found]:
                            emails_found.append({
                                'email': email,
                                'source': 'website_mailto',
                                'source_label': 'Website (mailto)',
                                'url': url,
                                'confidence': 95  # Higher confidence for mailto links
                            })
                
                if len(emails_found) >= 3:
                    break  # Got enough emails
                    
        except Exception as e:
            continue  # Skip failed pages
    
    return emails_found


def check_email_domain_match(email, company_name):
    """Check if email domain matches or is similar to company name"""
    if not email or not company_name:
        return 'unknown'
    
    # Extract domain from email
    domain = email.split('@')[-1].lower()
    domain_base = domain.split('.')[0]  # e.g., 'acme' from 'acme.co.uk'
    
    # Clean company name for comparison
    clean_name = company_name.lower()
    for suffix in [' limited', ' ltd', ' llp', ' plc', ' inc', ' corp', ' & co', ' and co']:
        clean_name = clean_name.replace(suffix, '')
    clean_name = clean_name.strip().replace(' ', '').replace(',', '').replace('.', '')
    
    # Check for match
    if domain_base in clean_name or clean_name in domain_base:
        return 'company'  # Email belongs to the company
    
    # Check for common accountant/auditor domains
    auditor_domains = ['kpmg', 'pwc', 'deloitte', 'ey', 'bdo', 'gt', 'mazars', 'rsm', 
                       'bakertilly', 'moorestephens', 'crowe', 'haysmacintyre', 'jeffreys']
    for auditor in auditor_domains:
        if auditor in domain_base:
            return 'auditor'
    
    # Check for common filing agent domains
    agent_domains = ['companieshouse', 'gov', 'hmrc', 'rapidformations', 'yourcompanyformations',
                     '1stformations', 'theformationscompany', 'jordans', 'inform']
    for agent in agent_domains:
        if agent in domain_base:
            return 'agent'
    
    return 'other'  # Unknown - could be accountant, solicitor, etc.




def find_free_emails(company_number, company_name, directors=None, company_domain=None):
    """Find emails using FREE methods only (Website scraping)"""
    all_emails = []
    verified_domain = None
    
    # Step 1: If we have a domain, use it; otherwise try to find/verify one
    if company_domain:
        verified_domain = company_domain
    else:
        # Try to find/verify a domain
        potential_domains = infer_domain_from_company_name(company_name)
        if potential_domains:
            for domain in potential_domains[:2]:
                if verify_domain_exists(domain):
                    verified_domain = domain
                    break
    
    # Step 2: Scrape the website for emails - COMPLETELY FREE
    if verified_domain:
        scraped_emails = scrape_website_for_emails(verified_domain)
        for email in scraped_emails:
            email['match_type'] = 'company'
            all_emails.append(email)
    
    # No more inferred emails - they waste verification credits
    
    return all_emails


def infer_domain_from_company_name(company_name):
    """Infer likely domain from company name - no API calls needed"""
    if not company_name:
        return None
    
    # Clean the company name
    clean_name = company_name.upper()
    for suffix in [' LIMITED', ' LTD', ' LLP', ' PLC', ' INC', ' CORPORATION', ' CORP', 
                   ' & CO', ' AND CO', ' UK', ' (UK)', ' SERVICES', ' GROUP', ' HOLDINGS']:
        clean_name = clean_name.replace(suffix, '')
    
    # Remove special characters and create slug
    clean_name = clean_name.strip().lower()
    for char in [' ', ',', '.', '&', "'", '-', '(', ')', '"']:
        clean_name = clean_name.replace(char, '')
    
    if not clean_name or len(clean_name) < 2:
        return None
    
    # Return potential domains to try (prioritize .co.uk for UK companies)
    return [
        f"{clean_name}.co.uk",
        f"{clean_name}.com",
        f"{clean_name}.uk",
        f"{clean_name}.org.uk",
    ]


def verify_domain_exists(domain):
    """Quick check if domain exists using DNS/HTTP - FREE"""
    import socket
    try:
        socket.gethostbyname(domain)
        return True
    except socket.gaierror:
        return False


def find_domain_free(company_name, company_number):
    """Find company domain using FREE methods only (Companies House + DNS)"""
    
    # Method 1: Infer domain from company name and verify via DNS
    potential_domains = infer_domain_from_company_name(company_name)
    if potential_domains:
        for domain in potential_domains:
            if verify_domain_exists(domain):
                return {
                    'domain': domain,
                    'source': 'inferred',
                    'verified': True
                }
    
    # Method 2: Check if we can find hints in company profile
    profile = get_company_profile(company_number)
    if profile:
        # Sometimes company name itself contains the domain
        company_name_lower = profile.get('company_name', '').lower()
        if '.co.uk' in company_name_lower or '.com' in company_name_lower:
            # Extract domain from company name
            import re
            domain_match = re.search(r'[\w-]+\.(co\.uk|com|uk|org)', company_name_lower)
            if domain_match:
                domain = domain_match.group(0)
                if verify_domain_exists(domain):
                    return {
                        'domain': domain,
                        'source': 'company_name',
                        'verified': True
                    }
    
    return None


def filter_csv(sic_filter, postcode_filter, limit, year_filter=''):
    """Filter the large CSV file based on criteria"""
    results = []
    count = 0
    
    # Determine which SIC codes to search for
    if sic_filter in SIC_CODES:
        target_sics = SIC_CODES[sic_filter]
    else:
        target_sics = [sic_filter]
    
    postcode_prefix = postcode_filter.upper().strip() if postcode_filter else None
    year_filter = year_filter.strip() if year_filter else None
    
    try:
        with open(CSV_PATH, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if count >= limit:
                    break
                
                # Check SIC codes
                sic_match = False
                matched_sic = None
                for i in range(1, 5):
                    sic_col = f'SICCode.SicText_{i}'
                    if sic_col in row and row[sic_col]:
                        sic_value = row[sic_col].strip()
                        for target in target_sics:
                            if sic_value.startswith(f'"{target}') or sic_value.startswith(target):
                                sic_match = True
                                matched_sic = target
                                break
                    if sic_match:
                        break
                
                if not sic_match:
                    continue
                
                # Check postcode
                postcode = row.get('RegAddress.PostCode', '').strip().strip('"')
                if postcode_prefix and not postcode.upper().startswith(postcode_prefix):
                    continue
                
                # Check company status - only include active companies
                status = row.get('CompanyStatus', '').strip().strip('"').lower()
                if status != 'active':
                    continue  # Skip dormant, dissolved, struck off, in administration, etc.
                
                # Check incorporation year if filter specified
                incorporation_date = row.get('IncorporationDate', '').strip().strip('"')
                if year_filter:
                    if year_filter == 'pre2022':
                        # Match any year before 2022
                        if '/2022' in incorporation_date or '/2023' in incorporation_date or '/2024' in incorporation_date or '/2025' in incorporation_date:
                            continue
                    else:
                        # Match specific year
                        if f'/{year_filter}' not in incorporation_date:
                            continue
                
                # Company matched filters
                company_number = row.get(' CompanyNumber', row.get('CompanyNumber', '')).strip().strip('"')
                company_name = row.get('CompanyName', '').strip().strip('"')
                
                results.append({
                    'company_name': company_name,
                    'company_number': company_number,
                    'address_line1': row.get('RegAddress.AddressLine1', row.get(' RegAddress.AddressLine1', '')).strip().strip('"'),
                    'address_line2': row.get(' RegAddress.AddressLine2', row.get('RegAddress.AddressLine2', '')).strip().strip('"'),
                    'town': row.get('RegAddress.PostTown', '').strip().strip('"'),
                    'county': row.get('RegAddress.County', '').strip().strip('"'),
                    'postcode': postcode,
                    'status': row.get('CompanyStatus', '').strip().strip('"'),
                    'sic_code': matched_sic,
                    'sic_description': SIC_DESCRIPTIONS.get(matched_sic, ''),
                    'incorporation_date': row.get('IncorporationDate', '').strip().strip('"'),
                    'directors': [],
                    'emails': []
                })
                count += 1
                
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return {'error': str(e)}
    
    return results


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


def clean_company_name_for_search(name):
    """Clean company name for matching - removes ACSP suffix and normalizes"""
    if not name:
        return name
    
    # Remove ACSP suffix (Authorised Corporate Service Provider designation)
    name = name.upper().strip()
    
    # Remove ACSP suffix with various spacing patterns
    import re
    name = re.sub(r'\s+ACSP\s*$', '', name)
    name = re.sub(r'\s+ACSP\s*\)$', ')', name)
    
    return name.strip()


def search_companies_house_by_name(company_name):
    """Search Companies House API for a company by name"""
    if not company_name or not COMPANIES_HOUSE_API_KEY:
        return None
    
    # Clean the name for search
    search_name = clean_company_name_for_search(company_name)
    
    url = "https://api.company-information.service.gov.uk/search/companies"
    try:
        response = requests.get(
            url,
            params={'q': search_name, 'items_per_page': 5},
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            # Try to find exact or close match
            search_clean = search_name.upper().replace(' ', '').replace('.', '').replace(',', '')
            
            for item in items:
                item_name = item.get('title', '').upper().replace(' ', '').replace('.', '').replace(',', '')
                # Check for exact match or very close match
                if item_name == search_clean or search_clean in item_name or item_name in search_clean:
                    return {
                        'company_number': item.get('company_number', ''),
                        'company_name': item.get('title', ''),
                        'company_status': item.get('company_status', ''),
                        'address': item.get('address', {}),
                        'date_of_creation': item.get('date_of_creation', '')
                    }
            
            # If no exact match, return first result if it looks close enough
            if items and len(items) > 0:
                first = items[0]
                first_name = first.get('title', '').upper().replace(' ', '').replace('.', '').replace(',', '')
                # Only accept if significant overlap
                if len(set(search_clean) & set(first_name)) > len(search_clean) * 0.7:
                    return {
                        'company_number': first.get('company_number', ''),
                        'company_name': first.get('title', ''),
                        'company_status': first.get('company_status', ''),
                        'address': first.get('address', {}),
                        'date_of_creation': first.get('date_of_creation', '')
                    }
        return None
    except Exception as e:
        print(f"Error searching Companies House for {company_name}: {e}")
        return None


@app.route('/api/import-match', methods=['POST'])
def import_match():
    """Match imported company names against Companies House data"""
    data = request.json
    imported_companies = data.get('companies', [])
    
    if not imported_companies:
        return jsonify({'error': 'No companies provided'}), 400
    
    # Build a lookup of company names to find - clean ACSP suffix
    names_to_find = {}
    original_names = {}  # Keep track of original names
    for company in imported_companies:
        original_name = company.get('import_name', '').upper().strip()
        clean_name = clean_company_name_for_search(original_name)
        if clean_name:
            names_to_find[clean_name] = company
            original_names[clean_name] = original_name  # Map clean to original
    
    results = []
    matched = 0
    not_found_list = []
    
    try:
        with open(CSV_PATH, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if not names_to_find:
                    break  # Found all companies
                
                company_name = row.get('CompanyName', '').strip().strip('"').upper()
                
                # Try to match against clean names
                if company_name in names_to_find:
                    imported_data = names_to_find.pop(company_name)
                    if company_name in original_names:
                        del original_names[company_name]
                    
                    # Check if active
                    status = row.get('CompanyStatus', '').strip().strip('"')
                    
                    company_number = row.get(' CompanyNumber', row.get('CompanyNumber', '')).strip().strip('"')
                    
                    # Build result with imported data
                    result = {
                        'company_name': row.get('CompanyName', '').strip().strip('"'),
                        'company_number': company_number,
                        'address_line1': row.get('RegAddress.AddressLine1', row.get(' RegAddress.AddressLine1', '')).strip().strip('"'),
                        'address_line2': row.get(' RegAddress.AddressLine2', row.get('RegAddress.AddressLine2', '')).strip().strip('"'),
                        'town': row.get('RegAddress.PostTown', '').strip().strip('"'),
                        'county': row.get('RegAddress.County', '').strip().strip('"'),
                        'postcode': row.get('RegAddress.PostCode', '').strip().strip('"'),
                        'status': status,
                        'sic_code': '',
                        'sic_description': '',
                        'incorporation_date': row.get('IncorporationDate', '').strip().strip('"'),
                        'directors': [],
                        'emails': [],
                        'domain': ''
                    }
                    
                    # Get SIC code
                    for i in range(1, 5):
                        sic_col = f'SICCode.SicText_{i}'
                        if sic_col in row and row[sic_col]:
                            sic_value = row[sic_col].strip().strip('"')
                            if sic_value:
                                result['sic_code'] = sic_value.split(' - ')[0] if ' - ' in sic_value else sic_value
                                result['sic_description'] = SIC_DESCRIPTIONS.get(result['sic_code'], '')
                                break
                    
                    # Add imported email if provided
                    if imported_data.get('import_email'):
                        result['emails'] = [{
                            'email': imported_data['import_email'],
                            'source': 'imported',
                            'source_label': 'Imported',
                            'match_type': 'unknown',
                            'confidence': 100
                        }]
                    
                    # Add imported website if provided
                    if imported_data.get('import_website'):
                        domain = imported_data['import_website']
                        # Clean the domain
                        domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
                        result['domain'] = domain
                        result['domain_source'] = 'imported'
                    
                    results.append(result)
                    matched += 1
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    # For companies not found in CSV, try Companies House API search
    api_found = 0
    for name, imported_data in list(names_to_find.items()):
        original_name = original_names.get(name, name)
        
        # Try searching Companies House API
        ch_result = search_companies_house_by_name(name)
        
        if ch_result and ch_result.get('company_number'):
            api_found += 1
            address = ch_result.get('address', {})
            result = {
                'company_name': ch_result.get('company_name', original_name),
                'company_number': ch_result.get('company_number', ''),
                'address_line1': address.get('address_line_1', ''),
                'address_line2': address.get('address_line_2', ''),
                'town': address.get('locality', ''),
                'county': address.get('region', ''),
                'postcode': address.get('postal_code', imported_data.get('import_location', '')),
                'status': ch_result.get('company_status', '').replace('_', ' ').title(),
                'sic_code': '',
                'sic_description': '',
                'incorporation_date': ch_result.get('date_of_creation', ''),
                'directors': [],
                'emails': [],
                'domain': ''
            }
            
            if imported_data.get('import_email'):
                result['emails'] = [{
                    'email': imported_data['import_email'],
                    'source': 'imported',
                    'source_label': 'Imported',
                    'match_type': 'unknown',
                    'confidence': 100
                }]
            
            if imported_data.get('import_website'):
                domain = imported_data['import_website']
                domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
                result['domain'] = domain
                result['domain_source'] = 'imported'
            
            results.append(result)
            matched += 1
            names_to_find.pop(name)
            time.sleep(0.2)  # Rate limit API calls
        else:
            not_found_list.append(original_name)
    
    # Add remaining companies that couldn't be found anywhere
    for name, imported_data in names_to_find.items():
        original_name = original_names.get(name, name)
        result = {
            'company_name': imported_data.get('import_name', original_name),
            'company_number': '',
            'address_line1': imported_data.get('import_location', ''),
            'address_line2': '',
            'town': '',
            'county': '',
            'postcode': '',
            'status': 'Not Found in Companies House',
            'sic_code': '',
            'sic_description': '',
            'incorporation_date': '',
            'directors': [],
            'emails': [],
            'domain': ''
        }
        
        if imported_data.get('import_email'):
            result['emails'] = [{
                'email': imported_data['import_email'],
                'source': 'imported',
                'source_label': 'Imported',
                'match_type': 'unknown',
                'confidence': 100
            }]
        
        if imported_data.get('import_website'):
            domain = imported_data['import_website']
            domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
            result['domain'] = domain
            result['domain_source'] = 'imported'
        
        results.append(result)
    
    return jsonify({
        'companies': results,
        'total': len(imported_companies),
        'matched': matched,
        'matched_via_api': api_found,
        'not_found': len(not_found_list),
        'not_found_names': not_found_list[:20]  # Return first 20 not found
    })


@app.route('/api/filter', methods=['POST'])
def filter_companies():
    """Filter companies based on criteria"""
    data = request.json
    sic_filter = data.get('sic', 'all_target')
    postcode_filter = data.get('postcode', '')
    year_filter = data.get('year', '')
    limit = min(int(data.get('limit', 100)), 1000)  # Max 1000 at a time
    
    results = filter_csv(sic_filter, postcode_filter, limit, year_filter)
    
    if isinstance(results, dict) and 'error' in results:
        return jsonify(results), 500
    
    return jsonify({
        'count': len(results),
        'companies': results
    })


@app.route('/api/enrich', methods=['POST'])
def enrich_companies():
    """Enrich selected companies with director information"""
    data = request.json
    company_numbers = data.get('company_numbers', [])
    
    enriched = []
    for company_number in company_numbers[:50]:  # Limit to 50 per request
        directors = get_officers(company_number)
        
        if isinstance(directors, dict) and directors.get('error') == 'rate_limited':
            time.sleep(1)  # Wait if rate limited
            directors = get_officers(company_number)
        
        enriched.append({
            'company_number': company_number,
            'directors': directors if isinstance(directors, list) else []
        })
        
        time.sleep(0.5)  # Rate limiting - Companies House allows 600/5min
    
    return jsonify({'enriched': enriched})


@app.route('/api/enrich-domains', methods=['POST'])
def enrich_domains():
    """Enrich companies with website domains - FREE methods first, Hunter.io as fallback"""
    data = request.json
    companies = data.get('companies', [])
    use_hunter_fallback = data.get('use_hunter', False)  # Only use Hunter if explicitly requested
    
    enriched = []
    domains_found = 0
    free_found = 0
    hunter_found = 0
    
    for company in companies[:100]:  # Can do more since free methods are fast
        company_name = company.get('company_name', '')
        company_number = company.get('company_number', '')
        
        # Try FREE method first (Companies House + DNS verification)
        domain_result = find_domain_free(company_name, company_number)
        
        if domain_result:
            domains_found += 1
            free_found += 1
            enriched.append({
                'company_number': company_number,
                'domain': domain_result.get('domain', ''),
                'source': domain_result.get('source', 'free'),
                'verified': domain_result.get('verified', False)
            })
        elif use_hunter_fallback:
            # Fallback to Hunter.io only if requested and free method failed
            hunter_result = find_company_domain(company_name)
            if hunter_result:
                domains_found += 1
                hunter_found += 1
                enriched.append({
                    'company_number': company_number,
                    'domain': hunter_result.get('domain', ''),
                    'source': 'hunter',
                    'verified': True
                })
                time.sleep(0.3)  # Rate limiting for Hunter
            else:
                enriched.append({
                    'company_number': company_number,
                    'domain': '',
                    'source': '',
                    'verified': False
                })
        else:
            enriched.append({
                'company_number': company_number,
                'domain': '',
                'source': '',
                'verified': False
            })
    
    return jsonify({
        'enriched': enriched,
        'domains_found': domains_found,
        'free_found': free_found,
        'hunter_found': hunter_found
    })


@app.route('/api/enrich-emails-free', methods=['POST'])
def enrich_emails_free():
    """Enrich companies with emails using FREE methods (Website scraping + Companies House)"""
    data = request.json
    companies = data.get('companies', [])
    
    enriched = []
    emails_found = 0
    scraped_count = 0
    website_count = 0
    ch_count = 0
    
    for company in companies[:50]:  # Limit due to website scraping time
        company_name = company.get('company_name', '')
        company_number = company.get('company_number', '')
        directors = company.get('directors', [])
        company_domain = company.get('domain', '')  # Use existing domain if we have it
        
        # Get free emails (website scraping only - no inferred)
        free_emails = find_free_emails(company_number, company_name, directors, company_domain)
        
        if free_emails:
            emails_found += len(free_emails)
            for email in free_emails:
                source = email.get('source', '')
                if 'website' in source:
                    scraped_count += 1
                    website_count += 1
        
        enriched.append({
            'company_number': company_number,
            'emails': free_emails
        })
        
        time.sleep(0.3)  # Be respectful when scraping
    
    return jsonify({
        'enriched': enriched,
        'emails_found': emails_found,
        'scraped_from_website': scraped_count,
        'website': website_count,
        'companies_house': ch_count
    })


@app.route('/api/enrich-emails', methods=['POST'])
def enrich_emails():
    """Enrich companies with email addresses using Hunter.io - ONLY for companies with inferred emails or no emails"""
    data = request.json
    companies = data.get('companies', [])
    
    enriched = []
    emails_found = 0
    skipped = 0
    
    for company in companies[:30]:  # Limit to 30 per request to conserve API credits
        company_name = company.get('company_name', '')
        directors = company.get('directors', [])
        company_domain = company.get('domain', '')
        existing_emails = company.get('emails', [])
        
        # Check if company already has REAL emails (from website scraping)
        has_real_emails = any(
            e.get('source') in ['website_scrape', 'website_mailto', 'imported'] 
            for e in existing_emails
        )
        
        # Skip companies that already have real emails - don't waste Hunter credits
        if has_real_emails:
            skipped += 1
            enriched.append({
                'company_number': company.get('company_number', ''),
                'emails': [],  # No new emails
                'skipped': True,
                'reason': 'Already has real emails'
            })
            continue
        
        company_emails = []
        
        # If we have a domain, use it directly for domain search
        if company_domain:
            try:
                response = requests.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={'domain': company_domain, 'api_key': HUNTER_API_KEY},
                    timeout=10
                )
                if response.status_code == 200:
                    data_resp = response.json()
                    for email_data in data_resp.get('data', {}).get('emails', [])[:3]:
                        company_emails.append({
                            'email': email_data.get('value', ''),
                            'first_name': email_data.get('first_name', ''),
                            'last_name': email_data.get('last_name', ''),
                            'position': email_data.get('position', ''),
                            'confidence': email_data.get('confidence', 0),
                            'source': 'domain_search',
                            'source_label': 'Hunter',
                            'match_type': 'company'
                        })
            except:
                pass
        
        # If no emails found yet, try to find the company domain
        if not company_emails:
            domain_result = search_company_domain(company_name)
            if domain_result:
                for email_data in domain_result.get('emails', [])[:3]:
                    company_emails.append({
                        'email': email_data.get('value', ''),
                        'first_name': email_data.get('first_name', ''),
                        'last_name': email_data.get('last_name', ''),
                        'position': email_data.get('position', ''),
                        'confidence': email_data.get('confidence', 0),
                        'source': 'domain_search',
                        'source_label': 'Hunter',
                        'match_type': 'company'
                    })
            else:
                # Try to find emails for each director using Email Finder
                for director in directors[:2]:
                    name = director.get('name', '')
                    if ',' in name:
                        parts = name.split(',')
                        last_name = parts[0].strip().title()
                        first_name = parts[1].strip().split()[0].title() if len(parts) > 1 else ''
                    else:
                        parts = name.split()
                        first_name = parts[0].title() if parts else ''
                        last_name = parts[-1].title() if len(parts) > 1 else ''
                    
                    if first_name and last_name:
                        email_result = find_email_for_person(first_name, last_name, company_name)
                        if email_result:
                            company_emails.append({
                                'email': email_result.get('email', ''),
                                'first_name': first_name,
                                'last_name': last_name,
                                'position': 'Director',
                                'confidence': email_result.get('confidence', 0),
                                'source': 'email_finder',
                                'source_label': 'Hunter',
                                'match_type': 'company'
                            })
                    
                    time.sleep(0.2)
        
        emails_found += len(company_emails)
        enriched.append({
            'company_number': company.get('company_number', ''),
            'emails': company_emails,
            'replaces_inferred': True  # These replace any inferred emails
        })
        
        time.sleep(0.3)
    
    return jsonify({
        'enriched': enriched,
        'emails_found': emails_found,
        'skipped': skipped
    })


@app.route('/api/export', methods=['POST'])
def export_csv():
    """Export enriched data to CSV - excludes invalid emails"""
    data = request.json
    companies = data.get('companies', [])
    filename = data.get('filename', 'enriched_companies.csv')
    
    output_path = os.path.join(os.path.dirname(CSV_PATH), filename)
    
    # Support up to 5 emails per company
    MAX_EMAILS = 5
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Build header with dynamic email columns
        header = [
            'Company Name', 'Company Number', 'Address Line 1', 'Address Line 2',
            'Town', 'County', 'Postcode', 'Status', 'SIC Code', 'SIC Description',
            'Incorporation Date', 'Website', 'Website Source', 'Director 1', 'Director 2', 'Director 3'
        ]
        for i in range(1, MAX_EMAILS + 1):
            header.extend([f'Email {i}', f'Email {i} Source', f'Email {i} Verified', f'Email {i} Score'])
        
        writer.writerow(header)
        
        for company in companies:
            directors = company.get('directors', [])
            director_names = [d.get('name', '') for d in directors[:3]]
            while len(director_names) < 3:
                director_names.append('')
            
            emails = company.get('emails', [])
            
            # Filter out invalid emails - don't export them
            valid_emails = [e for e in emails if e.get('verification_status', '').lower() != 'invalid']
            
            # Get email details with source, verification status, and score
            email_data = []
            for e in valid_emails[:MAX_EMAILS]:
                verification_status = e.get('verification_status', '')
                verification_score = e.get('verification_score', '')
                email_data.append({
                    'email': e.get('email', ''),
                    'source': e.get('source_label', e.get('source', '')),
                    'verified': verification_status if e.get('verified') else 'Not Verified',
                    'score': str(verification_score) if verification_score else ''
                })
            # Pad to MAX_EMAILS
            while len(email_data) < MAX_EMAILS:
                email_data.append({'email': '', 'source': '', 'verified': '', 'score': ''})
            
            # Build row
            row = [
                company.get('company_name', ''),
                company.get('company_number', ''),
                company.get('address_line1', ''),
                company.get('address_line2', ''),
                company.get('town', ''),
                company.get('county', ''),
                company.get('postcode', ''),
                company.get('status', ''),
                company.get('sic_code', ''),
                company.get('sic_description', ''),
                company.get('incorporation_date', ''),
                company.get('domain', ''),
                company.get('domain_source', ''),
                director_names[0],
                director_names[1],
                director_names[2]
            ]
            
            # Add all email columns
            for ed in email_data:
                row.extend([ed['email'], ed['source'], ed['verified'], ed['score']])
            
            writer.writerow(row)
    
    return jsonify({'success': True, 'path': output_path, 'count': len(companies)})


@app.route('/api/export-clean', methods=['POST'])
def export_clean_csv():
    """Export clean CSV - one row per email, CRM-ready format"""
    data = request.json
    companies = data.get('companies', [])
    filename = data.get('filename', 'clean_emails.csv')
    
    output_path = os.path.join(os.path.dirname(CSV_PATH), filename)
    
    total_emails = 0
    skipped_invalid = 0
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Simple header - one row per email
        writer.writerow([
            'Company Name', 'Company Number', 'First Name', 'Last Name', 
            'Email', 'Email Source', 'Verified Status', 'Verification Score'
        ])
        
        for company in companies:
            company_name = company.get('company_name', '')
            company_number = company.get('company_number', '')
            directors = company.get('directors', [])
            emails = company.get('emails', [])
            
            for email_data in emails:
                # Skip invalid emails
                verification_status = email_data.get('verification_status', '')
                if verification_status.lower() == 'invalid':
                    skipped_invalid += 1
                    continue
                
                email = email_data.get('email', '')
                if not email:
                    continue
                
                # Try to match email to a director
                first_name = ''
                last_name = ''
                
                # Check if email has associated name
                if email_data.get('first_name'):
                    first_name = email_data.get('first_name', '')
                    last_name = email_data.get('last_name', '')
                else:
                    # Try to match email to director by email pattern
                    email_lower = email.lower()
                    for director in directors:
                        name = director.get('name', '')
                        if ',' in name:
                            parts = name.split(',')
                            d_last = parts[0].strip()
                            d_first = parts[1].strip().split()[0] if len(parts) > 1 else ''
                        else:
                            parts = name.split()
                            d_first = parts[0] if parts else ''
                            d_last = parts[-1] if len(parts) > 1 else ''
                        
                        # Check if director name appears in email
                        if d_first.lower() in email_lower or d_last.lower() in email_lower:
                            first_name = d_first
                            last_name = d_last
                            break
                
                source = email_data.get('source_label', email_data.get('source', ''))
                verified = verification_status if email_data.get('verified') else 'Not Verified'
                score = str(email_data.get('verification_score', '')) if email_data.get('verification_score') else ''
                
                writer.writerow([
                    company_name,
                    company_number,
                    first_name,
                    last_name,
                    email,
                    source,
                    verified,
                    score
                ])
                total_emails += 1
    
    return jsonify({
        'success': True, 
        'path': output_path, 
        'total_emails': total_emails,
        'skipped_invalid': skipped_invalid
    })


@app.route('/api/sic-codes', methods=['GET'])
def get_sic_codes():
    """Return available SIC code filters"""
    return jsonify({
        'categories': {
            'accountants': {
                'name': 'Accountants',
                'codes': ['69201', '69203'],
                'description': 'Accounting, auditing & tax consultancy'
            },
            'management_consultancy': {
                'name': 'Management Consultancy',
                'codes': ['70229'],
                'description': 'Management consultancy activities'
            },
            'business_support': {
                'name': 'Business Support',
                'codes': ['82990', '82110'],
                'description': 'Business support & admin services'
            },
            'all_target': {
                'name': 'All Target SIC Codes',
                'codes': ['82990', '69201', '69203', '82110', '70229'],
                'description': 'All accountant-related SIC codes'
            }
        },
        'individual': SIC_DESCRIPTIONS
    })


def verify_email_hunter(email):
    """Verify a single email using Hunter.io Email Verifier API"""
    if not email or not HUNTER_API_KEY:
        return None
    
    url = "https://api.hunter.io/v2/email-verifier"
    try:
        response = requests.get(
            url,
            params={'email': email, 'api_key': HUNTER_API_KEY},
            timeout=15
        )
        if response.status_code == 200:
            data = response.json().get('data', {})
            return {
                'email': email,
                'status': data.get('status', 'unknown'),  # valid, invalid, accept_all, webmail, disposable, unknown
                'score': data.get('score', 0),  # 0-100 deliverability score
                'regexp': data.get('regexp', False),
                'gibberish': data.get('gibberish', False),
                'disposable': data.get('disposable', False),
                'webmail': data.get('webmail', False),
                'mx_records': data.get('mx_records', False),
                'smtp_server': data.get('smtp_server', False),
                'smtp_check': data.get('smtp_check', False),
                'accept_all': data.get('accept_all', False),
                'block': data.get('block', False),
                'sources': data.get('sources', [])
            }
        elif response.status_code == 400:
            return {'email': email, 'status': 'invalid', 'error': 'Invalid email format'}
        elif response.status_code == 429:
            return {'email': email, 'status': 'rate_limited', 'error': 'Rate limited'}
        else:
            return {'email': email, 'status': 'error', 'error': f'HTTP {response.status_code}'}
    except Exception as e:
        print(f"Error verifying email {email}: {e}")
        return {'email': email, 'status': 'error', 'error': str(e)}


@app.route('/api/verify-emails', methods=['POST'])
def verify_emails():
    """Verify email addresses using Hunter.io Email Verifier API"""
    data = request.json
    emails_to_verify = data.get('emails', [])
    
    if not emails_to_verify:
        return jsonify({'error': 'No emails provided'}), 400
    
    if not HUNTER_API_KEY:
        return jsonify({'error': 'Hunter API key not configured'}), 500
    
    results = []
    verified_count = 0
    valid_count = 0
    invalid_count = 0
    risky_count = 0
    
    for email_data in emails_to_verify[:100]:  # Limit to 100 per request
        email = email_data.get('email') if isinstance(email_data, dict) else email_data
        
        if not email:
            continue
        
        result = verify_email_hunter(email)
        
        if result:
            verified_count += 1
            status = result.get('status', 'unknown')
            
            if status == 'valid':
                valid_count += 1
            elif status == 'invalid':
                invalid_count += 1
            elif status in ['accept_all', 'webmail', 'unknown']:
                risky_count += 1
            
            # Include original data if provided
            if isinstance(email_data, dict):
                result['company_number'] = email_data.get('company_number', '')
                result['company_name'] = email_data.get('company_name', '')
                result['first_name'] = email_data.get('first_name', '')
                result['last_name'] = email_data.get('last_name', '')
            
            results.append(result)
        
        time.sleep(0.2)  # Rate limiting
    
    return jsonify({
        'results': results,
        'verified_count': verified_count,
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'risky_count': risky_count
    })


if __name__ == '__main__':
    app.run(debug=True, port=5000)

