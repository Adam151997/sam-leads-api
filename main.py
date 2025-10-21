import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="SAM Leads API",
    description="Complete US Business Database with 1.4M+ Records - Search by any field",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    database_url = os.getenv('DATABASE_URL')
    if database_url and 'sslmode' not in database_url:
        database_url += '?sslmode=require'
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

@app.get("/")
async def root():
    return {
        "message": "SAM Leads API - Complete Business Database",
        "version": "4.0.0",
        "records": "1.4M+ US Businesses",
        "searchable_fields": [
            "business_name", "state", "city", "zip_code", "naics_code",
            "duns_id", "dba_name", "address", "country", "poc_name",
            "business_type", "entity_structure", "website", "naics_sector"
        ],
        "endpoints": {
            "search": "/search?q=california",
            "advanced_search": "/leads?state=CA&naics=541611",
            "business_detail": "/business/{sam_id}",
            "stats": "/stats"
        }
    }

@app.get("/search")
async def search_businesses(q: str = Query(None), page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=1000)):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if q:
            search_query = "SELECT * FROM businesses WHERE \"LEGAL_BUSINESS_NAME\" ILIKE %s OR \"DBA_NAME\" ILIKE %s OR \"PHYSICAL_ADDRESS_CITY\" ILIKE %s OR \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" ILIKE %s OR \"PHYSICAL_ADDRESS_ZIPPOSTAL_CODE\" ILIKE %s OR \"PRIMARY_NAICS\" ILIKE %s OR \"BUS_TYPE_STRING\" ILIKE %s OR \"FULL_ADDRESS\" ILIKE %s OR \"GOVT_BUS_POC_FIRST_NAME\" ILIKE %s OR \"GOVT_BUS_POC_LAST_NAME\" ILIKE %s ORDER BY \"LEGAL_BUSINESS_NAME\" LIMIT %s OFFSET %s"
            search_pattern = f"%{q}%"
            params = [search_pattern] * 10 + [limit, offset]
        else:
            search_query = "SELECT * FROM businesses ORDER BY \"LEGAL_BUSINESS_NAME\" LIMIT %s OFFSET %s"
            params = [limit, offset]
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        if q:
            count_query = "SELECT COUNT(*) FROM businesses WHERE \"LEGAL_BUSINESS_NAME\" ILIKE %s OR \"DBA_NAME\" ILIKE %s OR \"PHYSICAL_ADDRESS_CITY\" ILIKE %s OR \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" ILIKE %s OR \"PHYSICAL_ADDRESS_ZIPPOSTAL_CODE\" ILIKE %s OR \"PRIMARY_NAICS\" ILIKE %s OR \"BUS_TYPE_STRING\" ILIKE %s OR \"FULL_ADDRESS\" ILIKE %s OR \"GOVT_BUS_POC_FIRST_NAME\" ILIKE %s OR \"GOVT_BUS_POC_LAST_NAME\" ILIKE %s"
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
            "query": q,
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

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
                conditions.append(f"\"{column}\" ILIKE %s")
                params.append(f"%{value}%")
        
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        base_query += " ORDER BY \"LEGAL_BUSINESS_NAME\" LIMIT %s OFFSET %s"
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
            "filters_applied": {k: v for k, v in locals().items() if v and k in ['business_name', 'state', 'city', 'zip_code', 'naics_code', 'duns_id', 'dba_name', 'country', 'business_type', 'entity_structure', 'naics_sector']},
            "total": total,
            "count": len(results),
            "page": page,
            "leads": results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/business/{sam_id}")
async def get_business_detail(sam_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM businesses WHERE \"UNIQUE_ENTITY_IDENTIFIER_SAM\" = %s', (sam_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {"success": True, "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/stats")
async def get_statistics():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as total FROM businesses")
        total = cursor.fetchone()["total"]
        
        cursor.execute("SELECT \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" as state, COUNT(*) as count FROM businesses WHERE \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" IS NOT NULL GROUP BY \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" ORDER BY count DESC LIMIT 10")
        top_states = cursor.fetchall()
        
        cursor.execute("SELECT \"PRIMARY_NAICS\" as naics, COUNT(*) as count FROM businesses WHERE \"PRIMARY_NAICS\" IS NOT NULL GROUP BY \"PRIMARY_NAICS\" ORDER BY count DESC LIMIT 10")
        top_naics = cursor.fetchall()
        
        cursor.execute("SELECT \"PHYSICAL_ADDRESS_CITY\" as city, COUNT(*) as count FROM businesses WHERE \"PHYSICAL_ADDRESS_CITY\" IS NOT NULL GROUP BY \"PHYSICAL_ADDRESS_CITY\" ORDER BY count DESC LIMIT 10")
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
