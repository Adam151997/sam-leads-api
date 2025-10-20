import os
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2.extras

app = FastAPI(title="SAM Leads API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

@app.get("/")
async def root():
    return {
        "message": "SAM Leads API", 
        "version": "1.0.0",
        "endpoints": {
            "leads": "/leads",
            "stats": "/stats", 
            "business_detail": "/leads/{id}"
        }
    }

@app.get("/leads/{sam_id}")
async def get_business(sam_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT * FROM usa_sam_leads 
            WHERE "UNIQUE_ENTITY_IDENTIFIER_SAM" = %s
        """, (sam_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            return {
                "success": True,
                "business": dict(result)
            }
        else:
            raise HTTPException(status_code=404, detail="Business not found")
            
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/leads")
async def get_leads(state: str = None, page: int = 1, limit: int = 50):
    try:
        offset = (page - 1) * limit
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if state:
            cursor.execute("""
                SELECT "UNIQUE_ENTITY_IDENTIFIER_SAM", "LEGAL_BUSINESS_NAME", 
                       "PHYSICAL_ADDRESS_CITY", "PHYSICAL_ADDRESS_PROVINCE_OR_STATE"
                FROM usa_sam_leads 
                WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """, (f"%{state}%", limit, offset))
        else:
            cursor.execute("""
                SELECT "UNIQUE_ENTITY_IDENTIFIER_SAM", "LEGAL_BUSINESS_NAME", 
                       "PHYSICAL_ADDRESS_CITY", "PHYSICAL_ADDRESS_PROVINCE_OR_STATE"
                FROM usa_sam_leads 
                ORDER BY "LEGAL_BUSINESS_NAME" 
                LIMIT %s OFFSET %s
            """, (limit, offset))
        
        results = cursor.fetchall()
        
        if state:
            cursor.execute("""
                SELECT COUNT(*) FROM usa_sam_leads 
                WHERE "PHYSICAL_ADDRESS_PROVINCE_OR_STATE" ILIKE %s
            """, (f"%{state}%",))
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
