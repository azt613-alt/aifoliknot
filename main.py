from live_scraper import main as run_scraper_task
import os
import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="AIפה לקנות API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

class BasketQuery(BaseModel):
    items: list[str]
    user_lat: float
    user_lon: float
    max_radius: float = 30.0

@app.get("/")
def health_check():
    return {"status": "online", "service": "AIפה לקנות API"}

@app.post("/api/live-compare")
def live_compare(query: BasketQuery):
    if not DATABASE_URL:
        return {"status": "error", "message": "Database not configured"}

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT s.chain_id, s.store_id, c.chain_name, s.store_name, s.address, s.lat, s.lon
        FROM stores s
        JOIN chains c ON s.chain_id = c.chain_id;
    """)
    stores = cur.fetchall()

    results = []

    for chain_id, store_id, chain_name, store_name, address, lat, lon in stores:
        total_price = 0.0
        details = []
        found_count = 0

        for item_text in query.items:
            cur.execute("""
                SELECT p.item_code, p.item_name, sp.item_price
                FROM products p
                JOIN store_prices sp ON p.item_code = sp.item_code
                WHERE sp.chain_id = %s AND sp.store_id = %s
                  AND (p.item_name % %s OR p.item_name ILIKE %s)
                ORDER BY similarity(p.item_name, %s) DESC
                LIMIT 1;
            """, (chain_id, store_id, item_text, f"%{item_text}%", item_text))
            
            match = cur.fetchone()
            if match:
                price = float(match[2])
                total_price += price
                found_count += 1
                details.append({
                    "query": item_text,
                    "matched_name": match[1],
                    "qty": 1,
                    "price": price
                })
            else:
                details.append({
                    "query": item_text,
                    "matched_name": "לא נמצא במלאי",
                    "qty": 1,
                    "price": None
                })

        if total_price > 0:
            results.append({
                "chain_name": chain_name,
                "store_name": store_name,
                "address": address,
                "lat": lat,
                "lon": lon,
                "total_price": round(total_price, 2),
                "found_items_count": f"{found_count}/{len(query.items)}",
                "details": details
            })

    cur.close()
    conn.close()
    results.sort(key=lambda x: x["total_price"])
    return {"status": "success", "results": results}
    @app.get("/api/run-scraper")
def trigger_scraper():
    run_scraper_task()
    return {"status": "success", "message": "Scraper completed successfully"}
