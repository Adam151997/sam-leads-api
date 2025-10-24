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
    version="6.0.1"
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

# =============================================
# DATABASE CONNECTION
# =============================================

def get_db_connection():
    database_url = "postgresql://postgres:TbwRoBSswDJFLuvTCdFRjUBOUVfzSegd@switchback.proxy.rlwy.net:14048/railway?sslmode=require"
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

# =============================================
# SAFE COLUMN DEFINITIONS - ONLY COLUMNS THAT EXIST
# =============================================

def get_public_fields():
    """Public fields - ONLY columns that definitely exist"""
    return [
        "UNIQUE_ENTITY_IDENTIFIER_SAM",
        "LEGAL_BUSINESS_NAME", 
        # REMOVED: "DBA_NAME" - doesn't exist in live DB
        "PHYSICAL_ADDRESS_CITY", 
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", 
        "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
        "PRIMARY_NAICS",
        "BUS_TYPE_STRING",
        "ENTITY_STRUCTURE"
    ]

def get_premium_fields():
    """Premium fields - ONLY columns that definitely exist"""
    return [
        "UNIQUE_ENTITY_IDENTIFIER_SAM",
        "LEGAL_BUSINESS_NAME", 
        "PHYSICAL_ADDRESS_CITY", 
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", 
        "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
        "PRIMARY_NAICS",
        "BUS_TYPE_STRING",
        "ENTITY_STRUCTURE"
        # Note: Removed DUNS, contact info until we verify they exist
    ]

# =============================================
# API ENDPOINTS - USING ONLY SAFE COLUMNS
# =============================================

@app.get("/")
async def root():
    return {
        "message": "SAM Business Database API",
        "version": "6.0.1",
        "records": "1.4M+ US Businesses",
        "status": "operational",
        "database": "Connected to PostgreSQL",
        "endpoints": {
            "search": "/search?q=company+name",
            "leads": "/leads?state=CA&city=Los+Angeles",
            "business_detail": "/business/{sam_id}",
            "stats": "/stats"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}

# PUBLIC SEARCH - Only safe columns
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
            # SIMPLIFIED SEARCH - Only using columns that definitely exist
            search_query = f"""
                SELECT {select_clause} 
                FROM businesses 
                WHERE "LEGAL_BUSINESS_NAME" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """
            search_pattern = f"%{q}%"
            params = [search_pattern] * 3 + [limit, offset]
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
                   OR "PHYSICAL_ADDRESS_CITY" ILIKE %s 
                   OR "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
            """
            count_params = [search_pattern] * 3
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
        
        # Build conditions based on provided filters - ONLY SAFE COLUMNS
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
        count_params = params[:-2] if conditions else []  # Remove limit and offset for count
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
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "statistics": {
                "total_businesses": total,
                "top_states": top_states,
                "top_naics_codes": top_naics
            }
        }
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return {"success": False, "error": str(e)}

# DEBUG ENDPOINT - Discover actual schema
@app.get("/debug/schema")
async def debug_schema():
    """Discover the actual column names in your database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all column names from the businesses table
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'businesses' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        
        # Get sample data to verify column names
        cursor.execute("SELECT * FROM businesses LIMIT 1")
        sample_row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total_columns": len(columns),
            "columns": [dict(col) for col in columns],
            "sample_row_columns": list(sample_row.keys()) if sample_row else []
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
