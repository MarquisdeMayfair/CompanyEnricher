"""
MCP (Model Context Protocol) Server for Company Data
Exposes tools for LLM to query database and enrich data
"""

import os
import json
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
from database import (
    get_db, search_companies, get_company_by_number,
    add_director, add_email, update_company_website,
    update_enrichment_status
)

load_dotenv()

app = Flask(__name__)
CORS(app)

HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
COMPANIES_HOUSE_API_KEY = os.getenv('COMPANIES_HOUSE_API_KEY')

# =============================================================================
# MCP Tool Definitions - These describe what tools the LLM can use
# =============================================================================

MCP_TOOLS = [
    {
        "name": "search_companies",
        "description": "Search for companies by name, SIC code, or postcode. Returns: company_name, company_number, address, sic_code. Use company_number with other tools.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_name": {
                    "type": "string",
                    "description": "Company name to search for (partial match)"
                },
                "sic_code": {
                    "type": "string", 
                    "description": "SIC code to filter by (e.g., '69201' for accountants)"
                },
                "postcode_prefix": {
                    "type": "string",
                    "description": "Postcode prefix to filter by (e.g., 'W1', 'EC1')"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (default 10)"
                }
            }
        }
    },
    {
        "name": "get_company_details",
        "description": "Get full company details. Returns: name, address, incorporation_date, status, sic_code, website, phone, confirmation_statement_due, accounts_due. Use for ANY company info questions.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_directors",
        "description": "Get directors/officers for a company. Returns: list of officers with name and role. NOTE: One person may have multiple roles (e.g. director AND secretary). Count unique NAMES not roles.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_company_website",
        "description": "Get or find the website URL for a company. Returns: website domain. Tries database first, then attempts to discover it.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                },
                "company_name": {
                    "type": "string",
                    "description": "Company name (helps with domain inference)"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_company_emails",
        "description": "Get known email addresses for a company. Returns: list of emails with source and verification status.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "find_email_for_person",
        "description": "Find email for a specific person at a company using Hunter.io. Returns: email address. COSTS CREDITS - only use when specifically asked for a person's email.",
        "parameters": {
            "type": "object",
            "properties": {
                "first_name": {
                    "type": "string",
                    "description": "Person's first name"
                },
                "last_name": {
                    "type": "string",
                    "description": "Person's last name"
                },
                "company_name": {
                    "type": "string",
                    "description": "Company name"
                },
                "domain": {
                    "type": "string",
                    "description": "Company domain (optional, will try to find if not provided)"
                }
            },
            "required": ["first_name", "last_name", "company_name"]
        }
    },
    {
        "name": "get_company_phone",
        "description": "Get the main phone number for a company. Returns: phone number and source.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_shareholders",
        "description": "Get persons with significant control (PSC) - shareholders who own 25%+ of the company. Returns: names, ownership percentages, whether individual or corporate.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_filing_history",
        "description": "Get company filing history - all documents filed with Companies House. Returns: list of filings with dates, types (accounts, confirmation statements, changes). Useful for due diligence.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of filings to return (default 10)"
                }
            },
            "required": ["company_number"]
        }
    },
    {
        "name": "get_company_charges",
        "description": "Get company charges (loans, mortgages, debentures). Returns: whether company has charges, details of any outstanding charges. Indicates financial obligations.",
        "parameters": {
            "type": "object",
            "properties": {
                "company_number": {
                    "type": "string",
                    "description": "The Companies House company number"
                }
            },
            "required": ["company_number"]
        }
    }
]


# =============================================================================
# Tool Implementation Functions
# =============================================================================

def tool_search_companies(company_name=None, sic_code=None, postcode_prefix=None, limit=10):
    """Search companies in database"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM companies WHERE company_status = 'Active'"
            params = []
            
            if company_name:
                query += " AND company_name LIKE ?"
                params.append(f"%{company_name.upper()}%")
            
            if sic_code:
                query += " AND (sic_code_1 = ? OR sic_code_2 = ?)"
                params.extend([sic_code, sic_code])
            
            if postcode_prefix:
                query += " AND postcode LIKE ?"
                params.append(f"{postcode_prefix.upper()}%")
            
            query += f" LIMIT {min(int(limit), 50)}"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                results.append({
                    "company_name": row["company_name"],
                    "company_number": row["company_number"],
                    "address": f"{row['address_line1'] or ''}, {row['post_town'] or ''} {row['postcode'] or ''}".strip(", "),
                    "sic_code": row["sic_code_1"],
                    "website": row["website"],
                    "phone": row["main_phone"]
                })
            
            return {"success": True, "count": len(results), "companies": results}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_company_details(company_number):
    """Get full company details including live data from Companies House API"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Get company from database
            cursor.execute("SELECT * FROM companies WHERE company_number = ?", (company_number,))
            company = cursor.fetchone()
            
            if not company:
                return {"success": False, "error": f"Company {company_number} not found"}
            
            # Get directors
            cursor.execute("SELECT * FROM directors WHERE company_number = ?", (company_number,))
            directors = [dict(d) for d in cursor.fetchall()]
            
            # Get emails
            cursor.execute("SELECT * FROM emails WHERE company_number = ?", (company_number,))
            emails = [dict(e) for e in cursor.fetchall()]
            
            # Get phones
            cursor.execute("SELECT * FROM phones WHERE company_number = ?", (company_number,))
            phones = [dict(p) for p in cursor.fetchall()]
            
            result = {
                "success": True,
                "company": {
                    "name": company["company_name"],
                    "number": company["company_number"],
                    "address": f"{company['address_line1'] or ''}, {company['post_town'] or ''} {company['postcode'] or ''}",
                    "status": company["company_status"],
                    "sic_code": company["sic_code_1"],
                    "incorporation_date": company["incorporation_date"],
                    "website": company["website"],
                    "phone": company["main_phone"]
                },
                "directors": directors,
                "emails": emails,
                "phones": phones
            }
            
            # Fetch live data from Companies House API (confirmation statement, accounts due, etc.)
            if COMPANIES_HOUSE_API_KEY:
                try:
                    api_response = requests.get(
                        f"https://api.company-information.service.gov.uk/company/{company_number}",
                        auth=(COMPANIES_HOUSE_API_KEY, ''),
                        timeout=5
                    )
                    if api_response.status_code == 200:
                        api_data = api_response.json()
                        # Add confirmation statement info
                        conf_stmt = api_data.get('confirmation_statement', {})
                        result["company"]["confirmation_statement_due"] = conf_stmt.get('next_due')
                        result["company"]["confirmation_statement_last"] = conf_stmt.get('last_made_up_to')
                        # Add accounts info
                        accounts = api_data.get('accounts', {})
                        result["company"]["accounts_due"] = accounts.get('next_due')
                        result["company"]["accounts_last"] = accounts.get('last_accounts', {}).get('made_up_to')
                except:
                    pass  # API call failed, continue with database data
            
            return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_directors(company_number):
    """Get directors, fetch from API if not in database"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if we have directors
            cursor.execute("SELECT * FROM directors WHERE company_number = ?", (company_number,))
            directors = cursor.fetchall()
            
            if directors:
                return {
                    "success": True,
                    "source": "database",
                    "directors": [{"name": d["name"], "role": d["officer_role"]} for d in directors]
                }
            
            # Fetch from Companies House API
            if COMPANIES_HOUSE_API_KEY:
                response = requests.get(
                    f"https://api.company-information.service.gov.uk/company/{company_number}/officers",
                    auth=(COMPANIES_HOUSE_API_KEY, ''),
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    officers = []
                    
                    # Get company_id for saving
                    cursor.execute("SELECT id FROM companies WHERE company_number = ?", (company_number,))
                    company_row = cursor.fetchone()
                    company_id = company_row['id'] if company_row else None
                    
                    for item in data.get('items', []):
                        if item.get('resigned_on'):
                            continue
                        
                        name = item.get('name', '')
                        officer_role = item.get('officer_role', '')
                        appointed_on = item.get('appointed_on', '')
                        
                        officers.append({"name": name, "role": officer_role})
                        
                        # Save to database if we have company_id
                        if company_id:
                            cursor.execute("""
                                INSERT OR IGNORE INTO directors (company_id, company_number, name, officer_role, appointed_on)
                                VALUES (?, ?, ?, ?, ?)
                            """, (company_id, company_number, name, officer_role, appointed_on))
                    
                    conn.commit()
                    
                    return {
                        "success": True,
                        "source": "companies_house_api",
                        "directors": officers
                    }
            
            return {"success": False, "error": "Directors not found and API unavailable"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_company_website(company_number, company_name=None):
    """Get or find company website"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT website, company_name FROM companies WHERE company_number = ?", (company_number,))
            company = cursor.fetchone()
            
            if not company:
                return {"success": False, "error": f"Company {company_number} not found"}
            
            if company["website"]:
                return {
                    "success": True,
                    "source": "database",
                    "website": company["website"]
                }
            
            # Try to infer domain from company name
            name = company_name or company["company_name"]
            if name:
                import socket
                import re
                
                # Clean company name
                clean_name = re.sub(r'\b(LIMITED|LTD|PLC|LLP|UK|SERVICES|GROUP|HOLDINGS)\b', '', name.upper())
                clean_name = re.sub(r'[^A-Z0-9]', '', clean_name).lower()
                
                for tld in ['.co.uk', '.com', '.uk']:
                    domain = f"{clean_name}{tld}"
                    try:
                        socket.gethostbyname(domain)
                        # Save to database
                        update_company_website(company_number, domain, 'inferred')
                        return {
                            "success": True,
                            "source": "inferred",
                            "website": domain
                        }
                    except:
                        pass
            
            return {"success": False, "error": "Website not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_company_emails(company_number):
    """Get emails from database"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT email, source, verification_status 
                FROM emails WHERE company_number = ?
            """, (company_number,))
            emails = cursor.fetchall()
            
            if emails:
                return {
                    "success": True,
                    "emails": [{"email": e["email"], "source": e["source"], "verified": e["verification_status"]} for e in emails]
                }
            
            return {"success": False, "error": "No emails found for this company"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_find_email_for_person(first_name, last_name, company_name, domain=None):
    """Find email for a person using Hunter.io"""
    if not HUNTER_API_KEY:
        return {"success": False, "error": "Hunter API key not configured"}
    
    try:
        # If no domain, try to find it first
        if not domain:
            # Try Hunter domain search
            search_url = f"https://api.hunter.io/v2/domain-search?company={company_name}&api_key={HUNTER_API_KEY}"
            response = requests.get(search_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                domain = data.get('data', {}).get('domain')
        
        if not domain:
            return {"success": False, "error": f"Could not find domain for {company_name}"}
        
        # Use Hunter Email Finder
        finder_url = f"https://api.hunter.io/v2/email-finder?domain={domain}&first_name={first_name}&last_name={last_name}&api_key={HUNTER_API_KEY}"
        response = requests.get(finder_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            email = data.get('data', {}).get('email')
            confidence = data.get('data', {}).get('score', 0)
            
            if email:
                return {
                    "success": True,
                    "email": email,
                    "confidence": confidence,
                    "source": "hunter.io"
                }
        
        return {"success": False, "error": "Email not found via Hunter"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_company_phone(company_number):
    """Get phone from database"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check main phone on company
            cursor.execute("SELECT main_phone FROM companies WHERE company_number = ?", (company_number,))
            company = cursor.fetchone()
            
            if company and company["main_phone"]:
                return {"success": True, "phone": company["main_phone"], "source": "database"}
            
            # Check phones table
            cursor.execute("SELECT phone, source FROM phones WHERE company_number = ?", (company_number,))
            phones = cursor.fetchall()
            
            if phones:
                return {
                    "success": True,
                    "phones": [{"phone": p["phone"], "source": p["source"]} for p in phones]
                }
            
            return {"success": False, "error": "No phone number found for this company"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_shareholders(company_number):
    """Get persons with significant control (shareholders with 25%+ ownership)"""
    if not COMPANIES_HOUSE_API_KEY:
        return {"success": False, "error": "Companies House API key not configured"}
    
    try:
        response = requests.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}/persons-with-significant-control",
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            shareholders = []
            
            for item in data.get('items', []):
                if item.get('ceased'):
                    continue
                    
                shareholder = {
                    "name": item.get('name', item.get('name_elements', {}).get('forename', '') + ' ' + item.get('name_elements', {}).get('surname', '')),
                    "type": "corporate" if "corporate" in item.get('kind', '') else "individual",
                    "notified_on": item.get('notified_on'),
                    "natures_of_control": item.get('natures_of_control', [])
                }
                
                # Add ownership details if available
                if 'identification' in item:
                    shareholder['registration_number'] = item['identification'].get('registration_number')
                    shareholder['legal_form'] = item['identification'].get('legal_form')
                
                shareholders.append(shareholder)
            
            return {
                "success": True,
                "shareholders": shareholders,
                "total": len(shareholders)
            }
        else:
            return {"success": False, "error": f"API returned status {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_filing_history(company_number, limit=10):
    """Get company filing history from Companies House"""
    if not COMPANIES_HOUSE_API_KEY:
        return {"success": False, "error": "Companies House API key not configured"}
    
    try:
        response = requests.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}/filing-history",
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            params={"items_per_page": limit},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            filings = []
            
            for item in data.get('items', []):
                filing = {
                    "date": item.get('date'),
                    "type": item.get('type'),
                    "category": item.get('category'),
                    "description": item.get('description', '').replace('-', ' ').title(),
                    "pages": item.get('pages')
                }
                filings.append(filing)
            
            return {
                "success": True,
                "filings": filings,
                "total": data.get('total_count', len(filings))
            }
        else:
            return {"success": False, "error": f"API returned status {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_get_company_charges(company_number):
    """Get company charges (loans, mortgages, debentures)"""
    if not COMPANIES_HOUSE_API_KEY:
        return {"success": False, "error": "Companies House API key not configured"}
    
    try:
        # First check if company has charges from profile
        profile_response = requests.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}",
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        
        if profile_response.status_code == 200:
            profile = profile_response.json()
            has_charges = profile.get('has_charges', False)
            
            if not has_charges:
                return {
                    "success": True,
                    "has_charges": False,
                    "message": "This company has no charges (loans or mortgages) registered"
                }
        
        # Get charge details
        response = requests.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}/charges",
            auth=(COMPANIES_HOUSE_API_KEY, ''),
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            charges = []
            
            for item in data.get('items', []):
                charge = {
                    "status": item.get('status'),
                    "created_on": item.get('created_on'),
                    "delivered_on": item.get('delivered_on'),
                    "charge_code": item.get('charge_code'),
                    "classification": item.get('classification', {}).get('description'),
                    "persons_entitled": [p.get('name') for p in item.get('persons_entitled', [])]
                }
                
                if item.get('satisfied_on'):
                    charge['satisfied_on'] = item.get('satisfied_on')
                    
                charges.append(charge)
            
            return {
                "success": True,
                "has_charges": len(charges) > 0,
                "charges": charges,
                "total": len(charges)
            }
        else:
            return {"success": False, "error": f"API returned status {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# Tool dispatcher
TOOL_FUNCTIONS = {
    "search_companies": tool_search_companies,
    "get_company_details": tool_get_company_details,
    "get_directors": tool_get_directors,
    "get_company_website": tool_get_company_website,
    "get_company_emails": tool_get_company_emails,
    "find_email_for_person": tool_find_email_for_person,
    "get_company_phone": tool_get_company_phone,
    "get_shareholders": tool_get_shareholders,
    "get_filing_history": tool_get_filing_history,
    "get_company_charges": tool_get_company_charges
}


# =============================================================================
# MCP API Endpoints
# =============================================================================

@app.route('/mcp/tools', methods=['GET'])
def list_tools():
    """Return list of available tools for LLM"""
    return jsonify({"tools": MCP_TOOLS})


@app.route('/mcp/execute', methods=['POST'])
def execute_tool():
    """Execute a tool and return results"""
    data = request.json
    tool_name = data.get('tool')
    parameters = data.get('parameters', {})
    
    print(f"🔧 MCP Tool called: {tool_name} with params: {parameters}")
    
    if tool_name not in TOOL_FUNCTIONS:
        return jsonify({"error": f"Unknown tool: {tool_name}"}), 400
    
    try:
        result = TOOL_FUNCTIONS[tool_name](**parameters)
        print(f"📋 Tool result: {result}")
        return jsonify(result)
    except Exception as e:
        print(f"❌ Tool error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/mcp/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({"status": "ok", "tools_available": len(MCP_TOOLS)})


if __name__ == '__main__':
    print("🔧 MCP Server starting on http://localhost:5002")
    print(f"📋 {len(MCP_TOOLS)} tools available")
    app.run(host='0.0.0.0', port=5002, debug=True)

