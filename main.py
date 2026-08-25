import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from live_scraper import main as run_scraper_task

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
    user_lat: float = 32.9691
    user_lon: float = 35.5422
    max_radius: float = 60.0

@app.get("/")
def health_check():
    return {"status": "online", "service": "AIפה לקנות API"}

@app.get("/api/debug")
def debug_status():
    if not DATABASE_URL:
        return {"status": "error", "message": "DATABASE_URL is missing"}
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM chains;")
        chains_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stores;")
        stores_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM products;")
        products_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM store_prices;")
        prices_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {
            "status": "connected",
            "chains": chains_count,
            "stores": stores_count,
            "products": products_count,
            "prices": prices_count
        }
    except Exception as e:
        return {"status": "error", "error_details": str(e)}

@app.get("/api/reset-stores")
@app.get("/api/run-scraper")
def trigger_full_sync():
    """הרצה ואיפוס של כל מאות הסניפים והמחירים מתוך live_scraper"""
    try:
        run_scraper_task()
        return {
            "status": "success",
            "message": "סנכרון מלא של כל הסניפים והמחירים הארציים הושלם בהצלחה!"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/live-compare")
def live_compare(query: BasketQuery):
    if not DATABASE_URL:
        return {"status": "error", "message": "DATABASE_URL not configured"}

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. שליפת כל הסניפים והרשתות הפעילים
        cur.execute("""
            SELECT s.chain_id, s.store_id, c.chain_name, s.store_name, s.address, s.lat, s.lon
            FROM stores s
            JOIN chains c ON s.chain_id = c.chain_id;
        """)
        stores = cur.fetchall()

        if not stores:
            cur.close()
            conn.close()
            return {"status": "success", "results": []}

        # 2. איתור קודי מוצרים לפי מילות החיפוש
        cleaned_items = [it.strip() for it in query.items if it.strip()]
        matched_items_map = {}

        for search_term in cleaned_items:
            cur.execute("""
                SELECT item_code, item_name 
                FROM products 
                WHERE item_name ILIKE %s 
                LIMIT 1;
            """, (f"%{search_term}%",))
            res = cur.fetchone()
            if res:
                matched_items_map[search_term] = {
                    "item_code": res["item_code"],
                    "item_name": res["item_name"]
                }

        # 3. שליפת כל המחירים בבת אחת עבור כל המוצרים שנמצאו
        product_codes = [v["item_code"] for v in matched_items_map.values()]
        prices_lookup = {}

        if product_codes:
            cur.execute("""
                SELECT chain_id, store_id, item_code, item_price
                FROM store_prices
                WHERE item_code = ANY(%s);
            """, (product_codes,))
            for row in cur.fetchall():
                key = (row["chain_id"], row["store_id"], row["item_code"])
                prices_lookup[key] = float(row["item_price"])

        cur.close()
        conn.close()

        # 4. הרכבת סלי הקניות בזיכרון השרת במהירות גבוהה
        results = []
        for s in stores:
            chain_id = s["chain_id"]
            store_id = s["store_id"]
            total_price = 0.0
            found_count = 0
            details = []

            for search_term in cleaned_items:
                match_info = matched_items_map.get(search_term)
                if match_info:
                    item_code = match_info["item_code"]
                    price = prices_lookup.get((chain_id, store_id, item_code))
                    if price is not None:
                        total_price += price
                        found_count += 1
                        details.append({
                            "query": search_term,
                            "matched_name": match_info["item_name"],
                            "qty": 1,
                            "price": price
                        })
                    else:
                        details.append({
                            "query": search_term,
                            "matched_name": match_info["item_name"],
                            "qty": 1,
                            "price": None
                        })
                else:
                    details.append({
                        "query": search_term,
                        "matched_name": "לא נמצא במלאי",
                        "qty": 1,
                        "price": None
                    })

            if found_count > 0:
                results.append({
                    "chain_name": s["chain_name"],
                    "store_name": s["store_name"],
                    "address": s["address"],
                    "lat": float(s["lat"]) if s["lat"] else 32.9691,
                    "lon": float(s["lon"]) if s["lon"] else 35.5422,
                    "total_price": round(total_price, 2),
                    "found_items_count": f"{found_count}/{len(cleaned_items)}",
                    "details": details
                })

        return {"status": "success", "results": results}

    except Exception as e:
        return {"status": "error", "message": f"Server error: {str(e)}"}
