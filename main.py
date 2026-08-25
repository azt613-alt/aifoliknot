import os
import psycopg2
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
    user_lat: float = 32.9790
    user_lon: float = 35.5480
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

@app.get("/api/run-scraper")
def trigger_scraper():
    try:
        run_scraper_task()
        return {"status": "success", "message": "Scraper completed successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/live-compare")
def live_compare(query: BasketQuery):
    if not DATABASE_URL:
        return {"status": "error", "message": "DATABASE_URL not configured"}

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

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

        results = []

        for chain_id, store_id, chain_name, store_name, address, lat, lon in stores:
            total_price = 0.0
            details = []
            found_count = 0

            for item_text in query.items:
                clean_item = item_text.strip()
                if not clean_item:
                    continue

                words = [w for w in clean_item.split() if len(w) > 1]
                if not words:
                    words = [clean_item]

                # חיפוש מדויק לפי מילות מפתח
                where_clauses = ["sp.chain_id = %s", "sp.store_id = %s"]
                params = [chain_id, store_id]
                for word in words:
                    where_clauses.append("p.item_name ILIKE %s")
                    params.append(f"%{word}%")

                cur.execute(f"""
                    SELECT p.item_code, p.item_name, sp.item_price
                    FROM products p
                    JOIN store_prices sp ON p.item_code = sp.item_code
                    WHERE {' AND '.join(where_clauses)}
                    LIMIT 1;
                """, tuple(params))

                match = cur.fetchone()

                # ניסיון חיפוש גמיש על המילה הראשונה אם לא נמצאה התאמה מלאה
                if not match and len(words) > 1:
                    cur.execute("""
                        SELECT p.item_code, p.item_name, sp.item_price
                        FROM products p
                        JOIN store_prices sp ON p.item_code = sp.item_code
                        WHERE sp.chain_id = %s AND sp.store_id = %s AND p.item_name ILIKE %s
                        LIMIT 1;
                    """, (chain_id, store_id, f"%{words[0]}%"))
                    match = cur.fetchone()

                if match:
                    price = float(match[2])
                    total_price += price
                    found_count += 1
                    details.append({
                        "query": clean_item,
                        "matched_name": match[1],
                        "qty": 1,
                        "price": price
                    })
                else:
                    details.append({
                        "query": clean_item,
                        "matched_name": "לא נמצא במלאי",
                        "qty": 1,
                        "price": None
                    })

            if found_count > 0:
                results.append({
                    "chain_name": chain_name,
                    "store_name": store_name,
                    "address": address,
                    "lat": float(lat) if lat else 32.9790,
                    "lon": float(lon) if lon else 35.5480,
                    "total_price": round(total_price, 2),
                    "found_items_count": f"{found_count}/{len(query.items)}",
                    "details": details
                })

        cur.close()
        conn.close()

        results.sort(key=lambda x: x["total_price"])
        return {"status": "success", "results": results}

    except Exception as e:
        return {"status": "error", "message": f"Query error: {str(e)}"}
