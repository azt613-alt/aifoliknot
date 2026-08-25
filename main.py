import os
import re
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

def split_smart_stream(raw_text: str) -> list[str]:
    """מפצל טקסט חופשי לרשימת פריטים גם ללא פסיקים או ירידות שורה"""
    text = raw_text.strip()
    if not text:
        return []

    # 1. אם יש פסיקים, נקודה-פסיק או שורות חדשות - נפצל לפיהם קודם
    if re.search(r'[,;\n]', text):
        chunks = re.split(r'[,;\n]+', text)
    else:
        chunks = [text]

    final_items = []
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue

        # 2. זיהוי גבולות כמות (לדוגמה: "4 חלב 2 קוטג 5% 3 קולה 1.5 ליטר")
        # מפצל לפני ספרה שלא מלווה בנקודה עשרונית (נפח) ולא אחרי אחוזים
        sub_items = re.split(r'(?<=[^\d%.\s])\s+(?=\d+\s*(?:יחידות|יח|x|\*|\s)\s*[א-ת])', chunk)
        
        if len(sub_items) > 1:
            final_items.extend([s.strip() for s in sub_items if s.strip()])
        else:
            # 3. אם מדובר ברצף מילים ללא ספרות כלל (למשל: "חלב ביצים קוטג לחם")
            words = chunk.split()
            if len(words) > 3 and not any(char.isdigit() for char in chunk):
                final_items.extend(words)
            else:
                final_items.append(chunk)

    return final_items

def parse_item_and_qty(raw_text: str):
    """מפריד כמות משם המוצר"""
    text = raw_text.strip()
    if not text:
        return "", 1

    # תבנית כמות בהתחלה (למשל: "4 חלב", "2x קוטג")
    match = re.match(r'^(\d+)\s*(?:יחידות|יח[\'"]?|x|\*|\s)?\s*(.+)$', text, re.IGNORECASE)
    if match:
        qty_str, name = match.groups()
        if not name.startswith('%') and name.strip():
            return name.strip(), max(1, int(qty_str))

    # תבנית כמות בסוף (למשל: "חלב x 4", "קוטג כפול 2")
    match = re.match(r'^(.+?)\s*(?:x|\*|כפול|יחידות|יח[\'"]?)\s*(\d+)$', text, re.IGNORECASE)
    if match:
        name, qty_str = match.groups()
        if name.strip():
            return name.strip(), max(1, int(qty_str))

    return text, 1

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
    try:
        run_scraper_task()
        return {"status": "success", "message": "סנכרון מלא הושלם בהצלחה!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/live-compare")
def live_compare(query: BasketQuery):
    if not DATABASE_URL:
        return {"status": "error", "message": "DATABASE_URL not configured"}

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. שליפת רשימת הסניפים
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

        # 2. פענוח חכם של כל קלט הפריטים
        raw_combined = " ".join(query.items)
        split_items = split_smart_stream(raw_combined)

        parsed_items = []
        for it in split_items:
            item_name, qty = parse_item_and_qty(it)
            if item_name:
                parsed_items.append({
                    "raw": it,
                    "search_term": item_name,
                    "qty": qty
                })

        # 3. איתור קודי המוצרים בקטלוג
        matched_items_map = {}
        for p in parsed_items:
            search_term = p["search_term"]
            if search_term not in matched_items_map:
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

        # 4. משיכת המחירים מכל הסניפים
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

        # 5. חישוב סלי הקניות
        results = []
        for s in stores:
            chain_id = s["chain_id"]
            store_id = s["store_id"]
            total_price = 0.0
            found_count = 0
            details = []

            for p in parsed_items:
                raw_query = p["raw"]
                search_term = p["search_term"]
                qty = p["qty"]
                match_info = matched_items_map.get(search_term)

                if match_info:
                    item_code = match_info["item_code"]
                    unit_price = prices_lookup.get((chain_id, store_id, item_code))
                    if unit_price is not None:
                        line_total = round(unit_price * qty, 2)
                        total_price += line_total
                        found_count += 1
                        details.append({
                            "query": raw_query,
                            "matched_name": match_info["item_name"],
                            "qty": qty,
                            "unit_price": unit_price,
                            "price": line_total
                        })
                    else:
                        details.append({
                            "query": raw_query,
                            "matched_name": match_info["item_name"],
                            "qty": qty,
                            "unit_price": None,
                            "price": None
                        })
                else:
                    details.append({
                        "query": raw_query,
                        "matched_name": "לא נמצא במלאי",
                        "qty": qty,
                        "unit_price": None,
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
                    "found_items_count": f"{found_count}/{len(parsed_items)}",
                    "details": details
                })

        return {"status": "success", "results": results}

    except Exception as e:
        return {"status": "error", "message": f"Server error: {str(e)}"}
