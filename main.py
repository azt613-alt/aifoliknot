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

# מילות עוגן נפוצות לזיהוי תחילת מוצר חדש ברצף מילים ללא פסיקים
PRODUCT_ANCHORS = {
    "חלב", "גבינה", "גבינת", "קוטג", "קוטג'", "ביצים", "ביצי", "חמאה", "שמנת", "יוגורט", "מעדן", "שוקו",
    "לחם", "חלה", "פיתות", "לחמניות", "בייגל", "עוגת", "עוגיות", "וופל", "פירורי", "מצות",
    "קולה", "קוקה", "ספרייט", "פנטה", "שוופס", "מים", "סודה", "מיץ", "פריגת", "תירוש", "בירה", "יין",
    "קפה", "תה", "נספרסו", "שוקולד", "במבה", "ביסלי", "תפוציפס", "תפוצ'יפס", "דוריטוס", "בייגלה", "קרקר", "ממרח", "נוטלה",
    "שמן", "טונה", "טחינה", "חומוס", "סלט", "מלפפונים", "זיתים", "תירס", "עגבניות", "רסק", "קטשופ", "מיונז", "חרדל", "סויה", "רוטב", "מלח", "פלפל", "פפריקה", "כמון", "כורכום", "אבקת",
    "אורז", "סוכר", "קמח", "פסטה", "ספגטי", "פתיתים", "קוסקוס", "עדשים", "שעועית", "קינואה", "שיבולת", "קורנפלקס", "כריות", "צ'יריוס", "גרנולה",
    "עגבניה", "מלפפון", "בצל", "תפוח", "בטטה", "גזר", "חסה", "כרוב", "כרובית", "ברוקולי", "קישוא", "חציל", "שום", "פטריות", "נענע", "פטרוזיליה", "כוסברה", "בננה", "תפוז", "לימון", "אבטיח", "מלון", "ענבים", "תות", "אבוקדו", "מנגו",
    "חזה", "שניצל", "כרעיים", "שוקיים", "כנפיים", "עוף", "פרגיות", "בשר", "בקר", "אנטריקוט", "סינטה", "אסאדו", "סלמון", "אמנון", "דניס", "דג", "טבעול", "המבורגר", "נקניקיות", "פסטרמה", "סלמי", "קבב",
    "סנפרוסט", "צ'יפס", "בצק", "מלוואח", "ג'חנון", "בורקס", "פיצה", "גלידת", "מגנום", "טילון",
    "שמפו", "מרכך", "תחליב", "סבון", "משחת", "מברשת", "מי", "דאודורנט", "ג'ל", "סכיני", "גילוח", "תחבושות", "טמפונים",
    "נייר", "מגבוני", "נוזל", "טבליות", "קפסולות", "אריאל", "פרסיל", "בדין", "לנור", "אקונומיקה", "סנו", "מסיר", "מטליות", "שקיות", "ניילון",
    "חיתולי", "האגיס", "פמפרס", "מטרנה", "סימילאק", "נוטרילון", "דייסת", "גרבר"
}

class BasketQuery(BaseModel):
    items: list[str]
    user_lat: float = 32.9691
    user_lon: float = 35.5422
    max_radius: float = 60.0

def parse_smart_shopping_stream(raw_text: str) -> list[dict]:
    """
    מפענח טבעי המפרק רצף טקסט למוצרים וכמויות,
    כולל מקרים ללא פסיקים כמו: '4 חלב 2 קוטג 3 קוקה קולה לחם אחיד'
    """
    text = raw_text.strip()
    if not text:
        return []

    # פיצול ראשוני לפי סימני פיסוק אם קיימים
    lines = re.split(r'[,;\n+]+', text)
    result_items = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        tokens = line.split()
        current_qty = 1
        current_words = []

        i = 0
        while i < len(tokens):
            token = tokens[i]

            # 1. האם הטוקן הוא מספר כמות עצמאי (למשל: "4", "2x", "3 יחידות")
            qty_match = re.match(r'^(\d+)(?:x|\*|יח|יחידות)?$', token, re.IGNORECASE)
            if qty_match and not token.endswith('%'):
                # אם כבר צברנו שם מוצר קודם - נשמור אותו
                if current_words:
                    result_items.append({
                        "raw": " ".join(current_words),
                        "search_term": " ".join(current_words),
                        "qty": current_qty
                    })
                    current_words = []

                current_qty = max(1, int(qty_match.group(1)))
                # דילוג על מילת יחידות אם היא מופיעה כמילה נפרדת (למשל: "3" "יחידות")
                if i + 1 < len(tokens) and tokens[i+1] in ["יחידות", "יח", "יח'", "בקבוקים", "קופסאות", "שקיות", "מארז"]:
                    i += 1
                i += 1
                continue

            # 2. האם הטוקן הוא מילת עוגן של מוצר חדש (ויש כבר מילים שנצברו לפריט קודם)
            clean_tok = re.sub(r'[^\u0590-\u05FFa-zA-Z]', '', token)
            if current_words and clean_tok in PRODUCT_ANCHORS:
                # מניעת פיצול שגוי בביטויים מחוברים נפוצים
                prev_word = current_words[-1]
                is_connected_phrase = (
                    (prev_word == "קוקה" and clean_tok == "קולה") or
                    (prev_word == "מי" and clean_tok in ["פה", "מיץ"]) or
                    (prev_word == "עוגת" and clean_tok == "הבית") or
                    (prev_word == "גבינת" and clean_tok in ["שמנת", "צהובה", "מוצרלה"]) or
                    (prev_word == "שמן" and clean_tok in ["זית", "קנולה", "סויה"])
                )

                if not is_connected_phrase:
                    result_items.append({
                        "raw": " ".join(current_words),
                        "search_term": " ".join(current_words),
                        "qty": current_qty
                    })
                    current_words = []
                    current_qty = 1  # איפוס כמות ברירת מחדל למוצר החדש

            current_words.append(token)
            i += 1

        if current_words:
            result_items.append({
                "raw": " ".join(current_words),
                "search_term": " ".join(current_words),
                "qty": current_qty
            })

    return result_items

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

        # 1. שליפת כל הסניפים והרשתות
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

        # 2. פענוח חכם של כל המחרוזת לפריטים וכמויות
        raw_combined = " ".join(query.items)
        parsed_items = parse_smart_shopping_stream(raw_combined)

        # 3. איתור חכם של המוצרים בקטלוג (Multi-Word Fuzzy Matching)
        matched_items_map = {}
        for p in parsed_items:
            search_term = p["search_term"]
            if search_term not in matched_items_map:
                words = [w for w in re.split(r'\s+', search_term) if len(w) > 1]
                
                if words:
                    # בניית שאילתה שבודקת שכל מילות המפתח מופיעות במוצר
                    conditions = " AND ".join(["item_name ILIKE %s" for _ in words])
                    params = [f"%{w}%" for w in words]
                    
                    cur.execute(f"""
                        SELECT item_code, item_name 
                        FROM products 
                        WHERE {conditions}
                        ORDER BY LENGTH(item_name) ASC
                        LIMIT 1;
                    """, tuple(params))
                    
                    res = cur.fetchone()
                    
                    # אם לא נמצא בהתאמה מלאה - נסה חיפוש לפי המילה הראשית הראשונה
                    if not res and len(words) > 1:
                        cur.execute("""
                            SELECT item_code, item_name 
                            FROM products 
                            WHERE item_name ILIKE %s 
                            ORDER BY LENGTH(item_name) ASC
                            LIMIT 1;
                        """, (f"%{words[0]}%",))
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

        # 5. הרכבת סלי הקניות
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
                            "query": f"{qty}x {raw_query}" if qty > 1 and not raw_query.startswith(str(qty)) else raw_query,
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
