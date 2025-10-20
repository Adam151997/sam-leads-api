import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="SAM Leads API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)

@app.get("/")
async def root():
    return {
        "message": "SAM Leads API",
        "version": "3.0.0",
        "records": "500,000+ businesses",
        "endpoints": {
            "search": "/leads?state=CA",
            "stats": "/stats",
            "details": "/leads/{id}"
        }
    }

@app.get("/leads/{sam_id}")
async def get_business(sam_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM businesses WHERE \"UNIQUE_ENTITY_IDENTIFIER_SAM\" = %s", (sam_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        if result:
            return {"success": True, "business": dict(result)}
        else:
            raise HTTPException(status_code=404, detail="Business not found")
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/leads")
async def get_leads(state: str = None, naics: str = None, page: int = 1, limit: int = 50):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query
        base_query = "SELECT \"UNIQUE_ENTITY_IDENTIFIER_SAM\", \"LEGAL_BUSINESS_NAME\", \"PHYSICAL_ADDRESS_CITY\", \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\", \"PRIMARY_NAICS\" FROM businesses"
        count_query = "SELECT COUNT(*) FROM businesses"
        params = []
        
        if state or naics:
            where_clause = " WHERE "
            conditions = []
            if state:
                conditions.append("\"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" ILIKE %s")
                params.append(f"%{state}%")
            if naics:
                conditions.append("\"PRIMARY_NAICS\" ILIKE %s")
                params.append(f"%{naics}%")
            where_clause += " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        base_query += " ORDER BY \"LEGAL_BUSINESS_NAME\" LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        # Get results
        cursor.execute(base_query, params)
        results = cursor.fetchall()
        
        # Get count
        count_params = params[:-2]
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        leads = []
        for row in results:
            leads.append({
                "sam_id": row["UNIQUE_ENTITY_IDENTIFIER_SAM"],
                "business_name": row["LEGAL_BUSINESS_NAME"],
                "city": row["PHYSICAL_ADDRESS_CITY"],
                "state": row["PHYSICAL_ADDRESS_PROVINCE_OR_STATE"],
                "naics_code": row["PRIMARY_NAICS"]
            })
        
        return {
            "success": True,
            "total": total,
            "count": len(leads),
            "page": page,
            "leads": leads
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/stats")
async def get_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\", COUNT(*) FROM businesses WHERE \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" IS NOT NULL GROUP BY \"PHYSICAL_ADDRESS_PROVINCE_OR_STATE\" ORDER BY COUNT(*) DESC LIMIT 10")
        top_states = [{"state": row[0], "count": row[1]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT COUNT(*) FROM businesses")
        total = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total_businesses": total,
            "top_states": top_states
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
