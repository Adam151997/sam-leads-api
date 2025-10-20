from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import psycopg2
import os

app = FastAPI(
    title="SAM Leads API",
    description="API for 1.4M+ USA SAM Business Leads",
    version="1.0.0"
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db-postgresql-nyc3-90775-do-user-27795217-0.d.db.ondigitalocean.com"),
        port=os.getenv("DB_PORT", "25060"),
        user=os.getenv("DB_USER", "doadmin"),
        password=os.getenv("DB_PASSWORD", "YOUR_PASSWORD"),
        database=os.getenv("DB_NAME", "defaultdb"),
        sslmode="require"
    )

@app.get("/")
async def root():
    return {"message": "SAM Leads API", "total_records": 1443180, "status": "active"}

@app.get("/leads")
async def get_leads(
    state: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if state:
            cursor.execute("""
                SELECT "UNIQUE_ENTITY_IDENTIFIER_SAM", "LEGAL_BUSINESS_NAME", 
                       "PHYSICAL_ADDRESS_CITY", "PHYSICAL_ADDRESS_PROVINCE_OR_STATE"
                FROM usa_sam_leads 
                WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s
            """, (f"%{state}%", limit))
        else:
            cursor.execute("""
                SELECT "UNIQUE_ENTITY_IDENTIFIER_SAM", "LEGAL_BUSINESS_NAME", 
                       "PHYSICAL_ADDRESS_CITY", "PHYSICAL_ADDRESS_PROVINCE_OR_STATE"
                FROM usa_sam_leads 
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s
            """, (limit,))
        
        results = cursor.fetchall()
        
        if state:
            cursor.execute("SELECT COUNT(*) FROM usa_sam_leads WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s", (f"%{state}%",))
        else:
            cursor.execute("SELECT COUNT(*) FROM usa_sam_leads")
        
        total = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        leads = []
        for row in results:
            leads.append({
                "sam_id": row[0],
                "business_name": row[1],
                "city": row[2],
                "state": row[3]
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
        
        cursor.execute("""
            SELECT "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", COUNT(*) 
            FROM usa_sam_leads 
            WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" IS NOT NULL
            GROUP BY "PHYSICAL_ADDRESS_PROVINCE_OR_STATE"
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        
        top_states = [{"state": row[0], "count": row[1]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT COUNT(*) FROM usa_sam_leads")
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