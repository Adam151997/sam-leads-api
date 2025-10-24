import os
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from io import StringIO
import secrets
import datetime
import time

app = FastAPI(
    title="SAM Leads API",
    description="Complete US Business Database with 1.4M+ Records - Search by any field",
    version="5.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================
# MANUAL API KEY MANAGEMENT SYSTEM
# =============================================

security = HTTPBearer()

# Dictionary to store all valid API keys
VALID_API_KEYS = {
    "demo_sam_key_123": {
        "plan": "premium", 
        "active": True,
        "customer": "Demo User",
        "created": "2024-01-01",
        "notes": "For testing and demo purposes"
    }
}

def validate_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Validate API key from the manual dictionary
    This function is called automatically on premium endpoints
    """
    api_key = credentials.credentials
    
    # Check if key exists and is active
    if api_key in VALID_API_KEYS and VALID_API_KEYS[api_key]["active"]:
        return api_key
    
    # If key is invalid
    raise HTTPException(
        status_code=401, 
        detail="Invalid API key. Please check your key or contact support."
    )

@app.post("/admin/generate-key")
async def generate_api_key(
    customer_name: str = Query(..., description="Customer name for this key"),
    plan_type: str = Query("premium", description="Plan type: premium, enterprise"),
    days_valid: int = Query(365, description="Days until key expires"),
    admin_secret: str = Query(..., description="Admin password for security")
):
    """
    Generate a new API key for a customer (MANUAL SYSTEM)
    Only you should have access to this endpoint!
    """
    # IMPORTANT: Change this password to something secure!
    if admin_secret != "your_sam_admin_password_123":
        raise HTTPException(status_code=401, detail="Invalid admin secret")
    
    # Generate secure random API key
    new_key = f"sam_{secrets.token_urlsafe(24)}"
    
    # Calculate expiry date
    expiry_date = (datetime.datetime.now() + datetime.timedelta(days=days_valid)).strftime("%Y-%m-%d")
    
    # Add to our manual dictionary
    VALID_API_KEYS[new_key] = {
        "plan": plan_type,
        "active": True,
        "customer": customer_name,
        "created": datetime.datetime.now().strftime("%Y-%m-%d"),
        "expires": expiry_date,
        "notes": f"Generated for {customer_name} on {datetime.datetime.now().strftime('%Y-%m-%d')}"
    }
    
    return {
        "success": True,
        "api_key": new_key,
        "customer": customer_name,
        "plan_type": plan_type,
        "expires_at": expiry_date,
        "message": "API key generated successfully. Send this key to your customer."
    }

@app.get("/admin/keys")
async def list_api_keys(admin_secret: str = Query(..., description="Admin password")):
    """
    List all API keys (for your admin use only)
    """
    if admin_secret != "your_sam_admin_password_123":
        raise HTTPException(status_code=401, detail="Invalid admin secret")
    
    keys_list = []
    for api_key, details in VALID_API_KEYS.items():
        keys_list.append({
            "api_key": f"{api_key[:10]}...",  # Only show first 10 chars for security
            "customer": details["customer"],
            "plan": details["plan"],
            "active": details["active"],
            "created": details["created"],
            "expires": details.get("expires", "Never"),
            "notes": details.get("notes", "")
        })
    
    return {
        "total_keys": len(keys_list),
        "active_keys": len([k for k in keys_list if k["active"]]),
        "keys": keys_list
    }

@app.post("/admin/disable-key")
async def disable_api_key(
    api_key: str = Query(..., description="API key to disable"),
    admin_secret: str = Query(..., description="Admin password")
):
    """
    Disable an API key (if customer stops paying, etc.)
    """
    if admin_secret != "your_sam_admin_password_123":
        raise HTTPException(status_code=401, detail="Invalid admin secret")
    
    if api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=404, detail="API key not found")
    
    VALID_API_KEYS[api_key]["active"] = False
    VALID_API_KEYS[api_key]["notes"] = f"Disabled on {datetime.datetime.now().strftime('%Y-%m-%d')}"
    
    return {"success": True, "message": f"API key disabled successfully"}

# =============================================
# EXISTING ENDPOINTS - NOW WITH PREMIUM FEATURES
# =============================================

def get_db_connection():
    database_url = os.getenv('DATABASE_URL')
    if database_url and 'sslmode' not in database_url:
        database_url += '?sslmode=require'
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

def get_public_fields():
    """Fields available to free users"""
    return [
        "LEGAL_BUSINESS_NAME", "DBA_NAME", "PHYSICAL_ADDRESS_CITY", 
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
        "PRIMARY_NAICS", "BUS_TYPE_STRING", "ENTITY_STRUCTURE", "NAICS_SECTOR",
        "PHYSICAL_ADDRESS_COUNTRY_CODE"
    ]

def get_premium_fields():
    """All fields including sensitive ones for premium users"""
    return "*"  # All fields

@app.get("/")
async def root():
    return {
        "message": "SAM Leads API - Complete Business Database",
        "version": "5.0.0",
        "records": "1.4M+ US Businesses",
        "access_tiers": {
            "public": "Basic business information",
            "premium": "Full contact details, DUNS numbers, export features (requires API key)"
        },
        "searchable_fields": [
            "business_name", "state", "city", "zip_code", "naics_code",
            "duns_id", "dba_name", "address", "country", "poc_name",
            "business_type", "entity_structure", "website", "naics_sector"
        ],
        "endpoints": {
            "search": "/search?q=california",
            "search_premium": "/search/premium?q=california (requires API key)",
            "advanced_search": "/leads?state=CA&naics=541611", 
            "advanced_search_premium": "/leads/premium?state=CA&naics=541611 (requires API key)",
            "business_detail": "/business/{sam_id}",
            "business_detail_premium": "/business/{sam_id}/premium (requires API key)",
            "export_csv": "/export/csv (requires API key)",
            "stats": "/stats"
        }
    }

# PUBLIC SEARCH - Basic fields only
@app.get("/search")
async def search_businesses(q: str = Query(None), page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=1000)):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = ", ".join([f'"{field}"' for field in get_public_fields()])
        
        if q:
            search_query = f"SELECT {public_fields} FROM businesses WHERE "LEGAL_BUSINESS_NAME" ILIKE %s OR "DBA_NAME" ILIKE %s OR "PHYSICAL_ADDRESS_CITY" ILIKE %s OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s OR "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE" ILIKE %s OR "PRIMARY_NAICS" ILIKE %s OR "BUS_TYPE_STRING" ILIKE %s OR "FULL_ADDRESS" ILIKE %s OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
            search_pattern = f"%{q}%"
            params = [search_pattern] * 10 + [limit, offset]
        else:
            search_query = f"SELECT {public_fields} FROM businesses ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
            params = [limit, offset]
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        if q:
            count_query = "SELECT COUNT(*) FROM businesses WHERE "LEGAL_BUSINESS_NAME" ILIKE %s OR "DBA_NAME" ILIKE %s OR "PHYSICAL_ADDRESS_CITY" ILIKE %s OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s OR "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE" ILIKE %s OR "PRIMARY_NAICS" ILIKE %s OR "BUS_TYPE_STRING" ILIKE %s OR "FULL_ADDRESS" ILIKE %s OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s"
            count_params = [search_pattern] * 10
        else:
            count_query = "SELECT COUNT(*) FROM businesses"
            count_params = []
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "public",
            "query": q,
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# PREMIUM SEARCH - All fields including sensitive data
@app.get("/search/premium")
async def search_businesses_premium(
    q: str = Query(None), 
    page: int = Query(1, ge=1), 
    limit: int = Query(50, ge=1, le=1000),
    api_key: str = Depends(validate_api_key)
):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if q:
            search_query = "SELECT * FROM businesses WHERE "LEGAL_BUSINESS_NAME" ILIKE %s OR "DBA_NAME" ILIKE %s OR "PHYSICAL_ADDRESS_CITY" ILIKE %s OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s OR "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE" ILIKE %s OR "PRIMARY_NAICS" ILIKE %s OR "BUS_TYPE_STRING" ILIKE %s OR "FULL_ADDRESS" ILIKE %s OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
            search_pattern = f"%{q}%"
            params = [search_pattern] * 10 + [limit, offset]
        else:
            search_query = "SELECT * FROM businesses ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
            params = [limit, offset]
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        if q:
            count_query = "SELECT COUNT(*) FROM businesses WHERE "LEGAL_BUSINESS_NAME" ILIKE %s OR "DBA_NAME" ILIKE %s OR "PHYSICAL_ADDRESS_CITY" ILIKE %s OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s OR "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE" ILIKE %s OR "PRIMARY_NAICS" ILIKE %s OR "BUS_TYPE_STRING" ILIKE %s OR "FULL_ADDRESS" ILIKE %s OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s"
            count_params = [search_pattern] * 10
        else:
            count_query = "SELECT COUNT(*) FROM businesses"
            count_params = []
        
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "premium",
            "query": q,
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# PUBLIC ADVANCED SEARCH - Basic fields only
@app.get("/leads")
async def advanced_search(
    business_name: str = Query(None),
    state: str = Query(None),
    city: str = Query(None),
    zip_code: str = Query(None),
    naics_code: str = Query(None),
    duns_id: str = Query(None),
    dba_name: str = Query(None),
    country: str = Query(None),
    business_type: str = Query(None),
    entity_structure: str = Query(None),
    naics_sector: str = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000)
):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = ", ".join([f'"{field}"' for field in get_public_fields()])
        base_query = f"SELECT {public_fields} FROM businesses"
        count_query = "SELECT COUNT(*) FROM businesses"
        conditions = []
        params = []
        
        filters = [
            ("LEGAL_BUSINESS_NAME", business_name),
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE", zip_code),
            ("PRIMARY_NAICS", naics_code),
            ("UNIQUE_ENTITY_IDENTIFIER_DUNS", duns_id),
            ("DBA_NAME", dba_name),
            ("PHYSICAL_ADDRESS_COUNTRY_CODE", country),
            ("BUS_TYPE_STRING", business_type),
            ("ENTITY_STRUCTURE", entity_structure),
            ("NAICS_SECTOR", naics_sector)
        ]
        
        for column, value in filters:
            if value:
                conditions.append(f""{column}" ILIKE %s")
                params.append(f"%{value}%")
        
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        base_query += " ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        count_params = params[:-2]
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "public",
            "filters_applied": {k: v for k, v in locals().items() if v and k in ['business_name', 'state', 'city', 'zip_code', 'naics_code', 'duns_id', 'dba_name', 'country', 'business_type', 'entity_structure', 'naics_sector']},
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# PREMIUM ADVANCED SEARCH - All fields including sensitive data
@app.get("/leads/premium")
async def advanced_search_premium(
    business_name: str = Query(None),
    state: str = Query(None),
    city: str = Query(None),
    zip_code: str = Query(None),
    naics_code: str = Query(None),
    duns_id: str = Query(None),
    dba_name: str = Query(None),
    country: str = Query(None),
    business_type: str = Query(None),
    entity_structure: str = Query(None),
    naics_sector: str = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    api_key: str = Depends(validate_api_key)
):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        base_query = "SELECT * FROM businesses"
        count_query = "SELECT COUNT(*) FROM businesses"
        conditions = []
        params = []
        
        filters = [
            ("LEGAL_BUSINESS_NAME", business_name),
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE", zip_code),
            ("PRIMARY_NAICS", naics_code),
            ("UNIQUE_ENTITY_IDENTIFIER_DUNS", duns_id),
            ("DBA_NAME", dba_name),
            ("PHYSICAL_ADDRESS_COUNTRY_CODE", country),
            ("BUS_TYPE_STRING", business_type),
            ("ENTITY_STRUCTURE", entity_structure),
            ("NAICS_SECTOR", naics_sector)
        ]
        
        for column, value in filters:
            if value:
                conditions.append(f""{column}" ILIKE %s")
                params.append(f"%{value}%")
        
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        base_query += " ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        count_params = params[:-2]
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "premium",
            "filters_applied": {k: v for k, v in locals().items() if v and k in ['business_name', 'state', 'city', 'zip_code', 'naics_code', 'duns_id', 'dba_name', 'country', 'business_type', 'entity_structure', 'naics_sector']},
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# PUBLIC BUSINESS DETAIL - Basic fields only
@app.get("/business/{sam_id}")
async def get_business_detail(sam_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = ", ".join([f'"{field}"' for field in get_public_fields()])
        cursor.execute(f'SELECT {public_fields} FROM businesses WHERE "UNIQUE_ENTITY_IDENTIFIER_SAM" = %s', (sam_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {"success": True, "access_level": "public", "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# PREMIUM BUSINESS DETAIL - All fields including sensitive data
@app.get("/business/{sam_id}/premium")
async def get_business_detail_premium(sam_id: str, api_key: str = Depends(validate_api_key)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM businesses WHERE "UNIQUE_ENTITY_IDENTIFIER_SAM" = %s', (sam_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {"success": True, "access_level": "premium", "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# CSV EXPORT - Premium feature only
@app.get("/export/csv")
async def export_businesses_csv(
    q: str = Query(None, description="Search term"),
    business_name: str = Query(None, description="Filter by business name"),
    state: str = Query(None, description="Filter by state"),
    city: str = Query(None, description="Filter by city"),
    zip_code: str = Query(None, description="Filter by zip code"),
    naics_code: str = Query(None, description="Filter by NAICS code"),
    shuffle: str = Query("random", description="Shuffle method: random, alphabetical, state_city, newest"),
    api_key: str = Depends(validate_api_key)
):
    """
    Export businesses data as CSV (PREMIUM FEATURE - REQUIRES API KEY)
    Returns full business details in CSV format with different results each time
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        query = "SELECT * FROM businesses WHERE 1=1"
        params = []
        
        if q:
            query += " AND ("LEGAL_BUSINESS_NAME" ILIKE %s OR "DBA_NAME" ILIKE %s OR "PHYSICAL_ADDRESS_CITY" ILIKE %s OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s OR "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE" ILIKE %s OR "PRIMARY_NAICS" ILIKE %s OR "BUS_TYPE_STRING" ILIKE %s OR "FULL_ADDRESS" ILIKE %s OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s)"
            search_pattern = f"%{q}%"
            params.extend([search_pattern] * 10)
        
        filters = [
            ("LEGAL_BUSINESS_NAME", business_name),
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE", zip_code),
            ("PRIMARY_NAICS", naics_code)
        ]
        
        for column, value in filters:
            if value:
                query += f" AND "{column}" ILIKE %s"
                params.append(f"%{value}%")
        
        # SMART SHUFFLING - Different results each time
        if shuffle == "random":
            query += " ORDER BY RANDOM()"
        elif shuffle == "alphabetical":
            query += " ORDER BY "LEGAL_BUSINESS_NAME""
        elif shuffle == "state_city":
            query += " ORDER BY "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", "PHYSICAL_ADDRESS_CITY", "LEGAL_BUSINESS_NAME""
        elif shuffle == "newest":
            query += " ORDER BY "UNIQUE_ENTITY_IDENTIFIER_SAM" DESC"
        else:
            # Default: random with time-based seed for variety
            random_seed = int(time.time()) % 1000
            query += f" ORDER BY MD5(CONCAT("UNIQUE_ENTITY_IDENTIFIER_SAM"::text, '{random_seed}'))"
        
        # Limit for safety
        query += " LIMIT 10000"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Create CSV
        output = StringIO()
        if results:
            # Get all column names
            columns = list(results[0].keys()) if results else []
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows([dict(row) for row in results])
        
        csv_content = output.getvalue()
        
        return {
            "filename": f"sam_businesses_export_{shuffle}_{int(time.time())}.csv",
            "content": csv_content,
            "count": len(results),
            "format": "csv",
            "shuffle_method": shuffle
        }
        
    finally:
        cursor.close()
        conn.close()

@app.get("/stats")
async def get_statistics():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as total FROM businesses")
        total = cursor.fetchone()["total"]
        
        cursor.execute("SELECT "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" as state, COUNT(*) as count FROM businesses WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" IS NOT NULL GROUP BY "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ORDER BY count DESC LIMIT 10")
        top_states = cursor.fetchall()
        
        cursor.execute("SELECT "PRIMARY_NAICS" as naics, COUNT(*) as count FROM businesses WHERE "PRIMARY_NAICS" IS NOT NULL GROUP BY "PRIMARY_NAICS" ORDER BY count DESC LIMIT 10")
        top_naics = cursor.fetchall()
        
        cursor.execute("SELECT "PHYSICAL_ADDRESS_CITY" as city, COUNT(*) as count FROM businesses WHERE "PHYSICAL_ADDRESS_CITY" IS NOT NULL GROUP BY "PHYSICAL_ADDRESS_CITY" ORDER BY count DESC LIMIT 10")
        top_cities = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "statistics": {
                "total_businesses": total,
                "top_states": top_states,
                "top_naics_codes": top_naics,
                "top_cities": top_cities
            }
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
