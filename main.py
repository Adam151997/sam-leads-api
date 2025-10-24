import os
import logging
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SAM Business Database API",
    description="Complete US Business Database with 1.4M+ Records - Premium Data Access",
    version="6.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================
# API KEY MANAGEMENT
# =============================================

security = HTTPBearer()

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
    api_key = credentials.credentials
    if api_key in VALID_API_KEYS and VALID_API_KEYS[api_key]["active"]:
        return api_key
    raise HTTPException(status_code=401, detail="Invalid API key.")

@app.post("/admin/generate-key")
async def generate_api_key(
    customer_name: str = Query(..., description="Customer name for this key"),
    plan_type: str = Query("premium", description="Plan type: premium, enterprise"),
    days_valid: int = Query(365, description="Days until key expires"),
    admin_secret: str = Query(..., description="Admin password for security")
):
    if admin_secret != "your_secure_admin_password_here_2024":
        raise HTTPException(status_code=401, detail="Invalid admin secret")
    
    new_key = f"sam_{secrets.token_urlsafe(24)}"
    expiry_date = (datetime.datetime.now() + datetime.timedelta(days=days_valid)).strftime("%Y-%m-%d")
    
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
        "message": "API key generated successfully."
    }

# =============================================
# DATABASE CONNECTION
# =============================================

def get_db_connection():
    database_url = "postgresql://postgres:TbwRoBSswDJFLuvTCdFRjUBOUVfzSegd@switchback.proxy.rlwy.net:14048/railway?sslmode=require"
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

# =============================================
# COLUMN DEFINITIONS - USING YOUR EXACT SCHEMA
# =============================================

def get_public_fields():
    """Public fields - basic business info"""
    return [
        "UNIQUE_ENTITY_IDENTIFIER_SAM",
        "LEGAL_BUSINESS_NAME", 
        "DBA_NAME",
        "PHYSICAL_ADDRESS_CITY", 
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", 
        "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
        "PRIMARY_NAICS",
        "BUS_TYPE_STRING",
        "ENTITY_STRUCTURE"
    ]

def get_premium_fields():
    """All fields including sensitive contact information"""
    return [
        "UNIQUE_ENTITY_IDENTIFIER_SAM",
        "UNIQUE_ENTITY_IDENTIFIER_DUNS",
        "LEGAL_BUSINESS_NAME", 
        "DBA_NAME",
        "PHYSICAL_ADDRESS_LINE_1",
        "PHYSICAL_ADDRESS_LINE_2",
        "PHYSICAL_ADDRESS_CITY", 
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", 
        "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
        "PHYSICAL_ADDRESS_COUNTRY_CODE",
        "GOVT_BUS_POC_FIRST_NAME",
        "GOVT_BUS_POC_LAST_NAME",
        "GOVT_BUS_POC_TITLE",
        "PRIMARY_NAICS",
        "BUS_TYPE_STRING",
        "ENTITY_STRUCTURE"
    ]

# =============================================
# API ENDPOINTS
# =============================================

@app.get("/")
async def root():
    return {
        "message": "SAM Business Database API",
        "version": "6.0.0",
        "records": "1.4M+ US Businesses",
        "status": "operational",
        "database": "Connected to PostgreSQL",
        "endpoints": {
            "search": "/search?q=company+name",
            "leads": "/leads?state=CA&city=Los+Angeles",
            "business_detail": "/business/{sam_id}",
            "premium_search": "/search/premium?q=company (requires API key)",
            "premium_leads": "/leads/premium?state=CA (requires API key)",
            "premium_detail": "/business/{sam_id}/premium (requires API key)",
            "export": "/export/csv (requires API key)",
            "stats": "/stats"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}

# PUBLIC SEARCH - Basic fields only
@app.get("/search")
async def search_businesses(
    q: str = Query(None, description="Search term"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000)
):
    """Search businesses with basic information (Free Tier)"""
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = get_public_fields()
        select_clause = ", ".join([f'"{field}"' for field in public_fields])
        
        if q:
            search_query = f"""
                SELECT {select_clause} 
                FROM businesses 
                WHERE "LEGAL_BUSINESS_NAME" ILIKE %s 
                   OR "DBA_NAME" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """
            search_pattern = f"%{q}%"
            params = [search_pattern] * 4 + [limit, offset]
        else:
            search_query = f"""
                SELECT {select_clause} 
                FROM businesses 
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """
            params = [limit, offset]
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        # Get total count
        if q:
            count_query = """
                SELECT COUNT(*) 
                FROM businesses 
                WHERE "LEGAL_BUSINESS_NAME" ILIKE %s 
                   OR "DBA_NAME" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
            """
            count_params = [search_pattern] * 4
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
        logger.error(f"Search error: {e}")
        return {"success": False, "error": str(e)}

# PREMIUM SEARCH - All fields including contact info
@app.get("/search/premium")
async def search_businesses_premium(
    q: str = Query(None, description="Search term"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    api_key: str = Depends(validate_api_key)
):
    """Search businesses with full contact information (Premium Tier)"""
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        premium_fields = get_premium_fields()
        select_clause = ", ".join([f'"{field}"' for field in premium_fields])
        
        if q:
            search_query = f"""
                SELECT {select_clause} 
                FROM businesses 
                WHERE "LEGAL_BUSINESS_NAME" ILIKE %s 
                   OR "DBA_NAME" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                   OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s
                   OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """
            search_pattern = f"%{q}%"
            params = [search_pattern] * 6 + [limit, offset]
        else:
            search_query = f"""
                SELECT {select_clause} 
                FROM businesses 
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """
            params = [limit, offset]
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        # Get total count
        if q:
            count_query = """
                SELECT COUNT(*) 
                FROM businesses 
                WHERE "LEGAL_BUSINESS_NAME" ILIKE %s 
                   OR "DBA_NAME" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                   OR "GOVT_BUS_POC_FIRST_NAME" ILIKE %s
                   OR "GOVT_BUS_POC_LAST_NAME" ILIKE %s
            """
            count_params = [search_pattern] * 6
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
        logger.error(f"Premium search error: {e}")
        return {"success": False, "error": str(e)}

# PUBLIC ADVANCED FILTERING
@app.get("/leads")
async def advanced_search(
    state: str = Query(None, description="State code (e.g., CA, NY)"),
    city: str = Query(None, description="City name"),
    zip_code: str = Query(None, description="ZIP code"),
    naics_code: str = Query(None, description="NAICS code"),
    business_type: str = Query(None, description="Business type"),
    entity_structure: str = Query(None, description="Entity structure"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000)
):
    """Advanced filtering with basic business info (Free Tier)"""
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = get_public_fields()
        select_clause = ", ".join([f'"{field}"' for field in public_fields])
        
        base_query = f"SELECT {select_clause} FROM businesses"
        count_query = "SELECT COUNT(*) FROM businesses"
        conditions = []
        params = []
        
        # Build conditions based on provided filters
        filters = [
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE", zip_code),
            ("PRIMARY_NAICS", naics_code),
            ("BUS_TYPE_STRING", business_type),
            ("ENTITY_STRUCTURE", entity_structure)
        ]
        
        for column, value in filters:
            if value:
                conditions.append(f'"{column}" ILIKE %s')
                params.append(f"%{value}%")
        
        # Apply conditions if any
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        # Add pagination
        base_query += ' ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s'
        params.extend([limit, offset])
        
        # Execute main query
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Execute count query
        count_params = params[:-2]  # Remove limit and offset for count
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "public",
            "filters_applied": {k: v for k, v in locals().items() if v and k in ['state', 'city', 'zip_code', 'naics_code', 'business_type', 'entity_structure']},
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        logger.error(f"Advanced search error: {e}")
        return {"success": False, "error": str(e)}

# PREMIUM ADVANCED FILTERING
@app.get("/leads/premium")
async def advanced_search_premium(
    state: str = Query(None, description="State code (e.g., CA, NY)"),
    city: str = Query(None, description="City name"),
    zip_code: str = Query(None, description="ZIP code"),
    naics_code: str = Query(None, description="NAICS code"),
    business_type: str = Query(None, description="Business type"),
    entity_structure: str = Query(None, description="Entity structure"),
    duns_id: str = Query(None, description="DUNS number"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    api_key: str = Depends(validate_api_key)
):
    """Advanced filtering with full contact information (Premium Tier)"""
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        premium_fields = get_premium_fields()
        select_clause = ", ".join([f'"{field}"' for field in premium_fields])
        
        base_query = f"SELECT {select_clause} FROM businesses"
        count_query = "SELECT COUNT(*) FROM businesses"
        conditions = []
        params = []
        
        # Build conditions including DUNS for premium
        filters = [
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PHYSICAL_ADDRESS_ZIPPOSTAL_CODE", zip_code),
            ("PRIMARY_NAICS", naics_code),
            ("BUS_TYPE_STRING", business_type),
            ("ENTITY_STRUCTURE", entity_structure),
            ("UNIQUE_ENTITY_IDENTIFIER_DUNS", duns_id)
        ]
        
        for column, value in filters:
            if value:
                conditions.append(f'"{column}" ILIKE %s')
                params.append(f"%{value}%")
        
        # Apply conditions if any
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        # Add pagination
        base_query += ' ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s OFFSET %s'
        params.extend([limit, offset])
        
        # Execute main query
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Execute count query
        count_params = params[:-2]  # Remove limit and offset for count
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "access_level": "premium",
            "filters_applied": {k: v for k, v in locals().items() if v and k in ['state', 'city', 'zip_code', 'naics_code', 'business_type', 'entity_structure', 'duns_id']},
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        logger.error(f"Premium advanced search error: {e}")
        return {"success": False, "error": str(e)}

# PUBLIC BUSINESS DETAIL
@app.get("/business/{sam_id}")
async def get_business_detail(sam_id: str):
    """Get basic business details by SAM ID (Free Tier)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        public_fields = get_public_fields()
        select_clause = ", ".join([f'"{field}"' for field in public_fields])
        
        cursor.execute(f'SELECT {select_clause} FROM businesses WHERE "UNIQUE_ENTITY_IDENTIFIER_SAM" = %s', (sam_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return {"success": True, "access_level": "public", "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except Exception as e:
        logger.error(f"Business detail error: {e}")
        return {"success": False, "error": str(e)}

# PREMIUM BUSINESS DETAIL
@app.get("/business/{sam_id}/premium")
async def get_business_detail_premium(sam_id: str, api_key: str = Depends(validate_api_key)):
    """Get full business details including contact info by SAM ID (Premium Tier)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        premium_fields = get_premium_fields()
        select_clause = ", ".join([f'"{field}"' for field in premium_fields])
        
        cursor.execute(f'SELECT {select_clause} FROM businesses WHERE "UNIQUE_ENTITY_IDENTIFIER_SAM" = %s', (sam_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return {"success": True, "access_level": "premium", "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except Exception as e:
        logger.error(f"Premium business detail error: {e}")
        return {"success": False, "error": str(e)}

# CSV EXPORT (PREMIUM FEATURE)
@app.get("/export/csv")
async def export_businesses_csv(
    state: str = Query(None),
    city: str = Query(None),
    naics_code: str = Query(None),
    limit: int = Query(1000, le=10000),
    api_key: str = Depends(validate_api_key)
):
    """Export business data as CSV (Premium Tier)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        premium_fields = get_premium_fields()
        select_clause = ", ".join([f'"{field}"' for field in premium_fields])
        
        base_query = f"SELECT {select_clause} FROM businesses"
        conditions = []
        params = []
        
        # Build conditions
        filters = [
            ("PHYSICAL_ADDRESS_PROVINCE_OR_STATE", state),
            ("PHYSICAL_ADDRESS_CITY", city),
            ("PRIMARY_NAICS", naics_code)
        ]
        
        for column, value in filters:
            if value:
                conditions.append(f'"{column}" ILIKE %s')
                params.append(f"%{value}%")
        
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
        
        base_query += ' ORDER BY "LEGAL_BUSINESS_NAME" LIMIT %s'
        params.append(limit)
        
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Create CSV
        output = StringIO()
        if results:
            columns = list(results[0].keys()) if results else []
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows([dict(row) for row in results])
        
        csv_content = output.getvalue()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "filename": f"sam_businesses_export_{int(time.time())}.csv",
            "content": csv_content,
            "count": len(results),
            "format": "csv"
        }
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        return {"success": False, "error": str(e)}

# STATISTICS ENDPOINT
@app.get("/stats")
async def get_statistics():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total count
        cursor.execute("SELECT COUNT(*) as total FROM businesses")
        total = cursor.fetchone()["total"]
        
        # Top states
        cursor.execute("""
            SELECT "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" as state, COUNT(*) as count 
            FROM businesses 
            WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" IS NOT NULL 
            GROUP BY "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" 
            ORDER BY count DESC LIMIT 10
        """)
        top_states = cursor.fetchall()
        
        # Top NAICS codes
        cursor.execute("""
            SELECT "PRIMARY_NAICS" as naics, COUNT(*) as count 
            FROM businesses 
            WHERE "PRIMARY_NAICS" IS NOT NULL 
            GROUP BY "PRIMARY_NAICS" 
            ORDER BY count DESC LIMIT 10
        """)
        top_naics = cursor.fetchall()
        
        # Top cities
        cursor.execute("""
            SELECT "PHYSICAL_ADDRESS_CITY" as city, COUNT(*) as count 
            FROM businesses 
            WHERE "PHYSICAL_ADDRESS_CITY" IS NOT NULL 
            GROUP BY "PHYSICAL_ADDRESS_CITY" 
            ORDER BY count DESC LIMIT 10
        """)
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
        logger.error(f"Stats error: {e}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
