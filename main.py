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

# מיפוי מובנה של מוצרי יסוד סטנדרטיים (ברירת מחדל ישראלית)
DEFAULT_STAPLES = {
    "חלב": "חלב תנובה 3% בקרטון 1 ליטר",
    "חלב 3%": "חלב תנובה 3% בקרטון 1 ליטר",
    "חלב 3": "חלב תנובה 3% בקרטון 1 ליטר",
    "חלב 1%": "חלב תנובה 1% בקרטון 1 ליטר",
    "חלב 1": "חלב תנובה 1% בקרטון 1 ליטר",
    "קוטג": "קוטג תנובה 5% 250 גרם",
    "קוטג'": "קוטג תנובה 5% 250 גרם",
    "קוטג 5%": "קוטג תנובה 5% 250 גרם",
    "קוטג 9%": "קוטג תנובה 9% 250 גרם",
    "גבינה לבנה": "גבינה לבנה תנובה 5% 250 גרם",
    "גבינה לבנה 5%": "גבינה לבנה תנובה 5% 250 גרם",
    "גבינה צהובה": "גבינה צהובה עמק 28% תנובה 200 גרם",
    "צהובה": "גבינה צהובה עמק 28% תנובה 200 גרם",
    "עמק": "גבינה צהובה עמק 28% תנובה 200 גרם",
    "ביצים": "ביצים L מארז 12 יחידות",
    "ביצים l": "ביצים L מארז 12 יחידות",
    "ביצים xl": "ביצים XL מארז 12 יחידות",
    "ביצים m": "ביצים M מארז 12 יחידות",
    "לחם": "לחם אחיד פרוס 750 גרם",
    "לחם אחיד": "לחם אחיד פרוס 750 גרם",
    "חלה": "חלה לשבת קלועה 500 גרם",
    "פיתות": "פיתות טריות מארז 10 יחידות",
    "קולה": "קוקה קולה 1.5 ליטר",
    "קוקה קולה": "קוקה קולה 1.5 ליטר",
    "זירו": "קוקה קולה זירו 1.5 ליטר",
    "קולה זירו": "קוקה קולה זירו 1.5 ליטר",
    "ספרייט זירו": "ספרייט זירו 1.5 ליטר",
    "שמן": "שמן קנולה מזוכך 1 ליטר",
    "שמן קנולה": "שמן קנולה מזוכך 1 ליטר",
    "שמן זית": "שמן זית כתית מעולה 750 מל",
    "סוכר": "סוכר לבן 1 קג",
    "קמח": "קמח חיטה לבן 1 קג",
    "אורז": "אורז פרסי קלאסי 1 קג",
    "פסטה": "פסטה פרפקטו אסם פנה 500 גרם",
    "ספגטי": "פסטה ברילה ספגטי מס 5 500 גרם",
    "פתיתים": "פתיתים אפויים קוסקוס אסם 500 גרם",
    "חמאה": "חמאה תנובה 100 גרם",
    "שמנת חמוצה": "שמנת חמוצה 15% תנובה 200 מל",
    "שמנת מתוקה": "שמנת מתוקה 38% השף הלבן 250 מל",
    "קפה": "קפה נמס עלית 200 גרם פחית",
    "נס קפה": "קפה נמס עלית 200 גרם פחית",
    "קפה נמס": "קפה נמס עלית 200 גרם פחית",
    "קפה שחור": "קפה שחור טורקי עלית 100 גרם",
    "טורקי": "קפה שחור טורקי עלית 100 גרם",
    "טסטרס צ'ויס": "קפה טסטרס צ'ויס נסטלה 200 גרם",
    "תה": "תה ויסוצקי קלאסי 100 שקיקים",
    "במבה": "במבה אסם קלאסית 80 גרם",
    "ביסלי": "ביסלי גריל אסם 70 גרם",
    "תפוציפס": "תפוצ'יפס קלאסי עלית 50 גרם",
    "תפוצ'יפס": "תפוצ'יפס קלאסי עלית 50 גרם",
    "שוקולד": "שוקולד פרה חלב עלית 100 גרם",
    "נוטלה": "ממרח נוטלה 750 גרם",
    "חזה עוף": "חזה עוף פרוס לשניצל טרי 1 קג",
    "שניצל": "חזה עוף פרוס לשניצל טרי 1 קג",
    "עוף": "עוף שלם טרי 1 קג",
    "כרעיים": "כרעיים עוף טרי 1 קג",
    "בשר": "בשר בקר טחון טרי 1 קג",
    "בשר טחון": "בשר בקר טחון טרי 1 קג",
    "טחון": "בשר בקר טחון טרי 1 קג",
    "טונה": "טונה סטארקיסט בשמן מארז 4 יחידות",
    "טחינה": "טחינה גולמית אל ארז 500 גרם",
    "חומוס": "חומוס צבר קלאסי 500 גרם",
    "קטשופ": "קטשופ אסם 750 גרם",
    "מיונז": "מיונז קלאסי הלמנס 405 גרם",
    "עגבניות": "עגבניות חממה טריות 1 קג",
    "עגבניה": "עגבניות חממה טריות 1 קג",
    "מלפפונים": "מלפפון שדה טרי 1 קג",
    "מלפפון": "מלפפון שדה טרי 1 קג",
    "בצל": "בצל יבש 1 קג",
    "תפוחי אדמה": "תפוח אדמה לבן שקית 2 קג",
    "תפוח אדמה": "תפוח אדמה לבן שקית 2 קג",
    "בטטה": "בטטה מובחרת 1 קג",
    "גזר": "גזר ארוז שקית 1 קג",
    "נייר טואלט": "מארז נייר טואלט לילי 30 גלילים",
    "נוזל כלים": "נוזל כלים פיירי 650 מל",
    "שמפו": "שמפו פינוק 700 מל",
    "משחת שיניים": "משחת שיניים קולגייט 100 מל",
    "חיתולים": "חיתולי האגיס אקסטרה קר מידה 4",
    "מטרנה": "מטרנה אקסטרה קר שלב 1 700 גרם"
}

# חלופות מיוחדות שאינן ברירת מחדל (יסוננו החוצה אם המשתמש לא ביקש אותן מפורשות)
SPECIAL_MODIFIERS = [
    "סויה", "שקדים", "שיבולת שועל", "קוקוס", "אורז", "ללא לקטוז",
    "אורגני", "טבעוני", "עיזים", "כבשים", "ללא גלוטן", "צמחי"
]

PRODUCT_ANCHORS = {
    "חלב", "גבינה", "גבינת", "קוטג", "קוטג'", "ביצים", "ביצי", "חמאה", "שמנת", "יוגורט", "מעדן", "שוקו",
    "לחם", "חלה", "פיתות", "לחמניות", "עוגיות",
    "קולה", "קוקה", "ספרייט", "פנטה", "מים", "סודה", "מיץ", "בירה", "יין",
    "קפה", "תה", "שוקולד", "במבה", "ביסלי", "תפוציפס", "תפוצ'יפס", "נוטלה",
    "שמן", "טונה", "טחינה", "חומוס", "סלט", "מלפפונים", "זיתים", "עגבניות", "קטשופ", "מיונז", "רוטב", "מלח",
    "אורז", "סוכר", "קמח", "פסטה", "ספגטי", "פתיתים",
    "עגבניה", "מלפפון", "בצל", "תפוח", "בטטה", "גזר", "חסה", "כרוב", "קישוא", "חציל", "שום", "בננה",
    "חזה", "שניצל", "כרעיים", "שוקיים", "עוף", "בשר", "בקר", "אנטריקוט", "סלמון", "דג", "טבעול", "נקניקיות",
    "סנפרוסט", "שמפו", "סבון", "משחת", "נייר", "נוזל", "חיתולי", "מטרנה"
}

class BasketQuery(BaseModel):
    items: list[str]
    user_lat: float = 32.9691
    user_lon: float = 35.5422
    max_radius: float = 60.0

def parse_smart_shopping_stream(raw_text: str) -> list[dict]:
    text = raw_text.strip()
    if not text:
        return []

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

            qty_match = re.match(r'^(\d+)(?:x|\*|יח|יחידות)?$', token, re.IGNORECASE)
            if qty_match and not token.endswith('%'):
                if current_words:
                    result_items.append({
                        "raw": " ".join(current_words),
                        "search_term": " ".join(current_words),
                        "qty": current_qty
                    })
                    current_words = []

                current_qty = max(1, int(qty_match.group(1)))
                if i + 1 < len(tokens) and tokens[i+1] in ["יחידות", "יח", "יח'", "בקבוקים", "קופסאות", "שקיות", "מארז"]:
                    i += 1
                i += 1
                continue

            clean_tok = re.sub(r'[^\u0590-\u05FFa-zA-Z]', '', token)
            if current_words and clean_tok in PRODUCT_ANCHORS:
                prev_word = current_words[-1]
                is_connected = (
                    (prev_word == "קוקה" and clean_tok == "קולה") or
                    (prev_word == "גבינת" and clean_tok in ["שמנת", "צהובה", "מוצרלה"]) or
                    (prev_word == "שמן" and clean_tok in ["זית", "קנולה", "סויה"])
                )

                if not is_connected:
                    result_items.append({
                        "raw": " ".join(current_words),
                        "search_term": " ".join(current_words),
                        "qty": current_qty
                    })
                    current_words = []
                    current_qty = 1

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

        # 1. שליפת סניפים
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

        # 2. פענוח חכם של רשימת המוצרים
        raw_combined = " ".join(query.items)
        parsed_items = parse_smart_shopping_stream(raw_combined)

        # 3. איתור חכם של המוצרים בקטלוג
        matched_items_map = {}
        for p in parsed_items:
            search_term = p["search_term"].strip()
            clean_term = re.sub(r'[\'"]', '', search_term).strip()

            if search_term in matched_items_map:
                continue

            # א. בדיקה במיפוי מוצרי יסוד ישראליים נפוצים
            if clean_term in DEFAULT_STAPLES or search_term in DEFAULT_STAPLES:
                target_exact_name = DEFAULT_STAPLES.get(clean_term, DEFAULT_STAPLES.get(search_term))
                cur.execute("SELECT item_code, item_name FROM products WHERE item_name ILIKE %s LIMIT 1;", (f"%{target_exact_name}%",))
                res = cur.fetchone()
                if res:
                    matched_items_map[search_term] = {"item_code": res["item_code"], "item_name": res["item_name"]}
                    continue

            # ב. חיפוש דינמי עם סינון חלופות מיוחדות
            words = [w for w in re.split(r'\s+', clean_term) if len(w) > 1]
            if words:
                conditions = ["item_name ILIKE %s" for _ in words]
                params = [f"%{w}%" for w in words]

                # חסימת סויה/שקדים/אורגני אם המשתמש לא ביקש אותם במפורש
                for mod in SPECIAL_MODIFIERS:
                    if mod not in search_term:
                        conditions.append("item_name NOT ILIKE %s")
                        params.append(f"%{mod}%")

                where_clause = " AND ".join(conditions)
                cur.execute(f"""
                    SELECT item_code, item_name 
                    FROM products 
                    WHERE {where_clause}
                    ORDER BY LENGTH(item_name) ASC
                    LIMIT 1;
                """, tuple(params))
                res = cur.fetchone()

                # ג. ניסיון שני ללא סינון שלילי במידה ולא נמצא דבר
                if not res:
                    basic_conditions = " AND ".join(["item_name ILIKE %s" for _ in words])
                    cur.execute(f"""
                        SELECT item_code, item_name 
                        FROM products 
                        WHERE {basic_conditions}
                        ORDER BY LENGTH(item_name) ASC
                        LIMIT 1;
                    """, tuple([f"%{w}%" for w in words]))
                    res = cur.fetchone()

                if res:
                    matched_items_map[search_term] = {"item_code": res["item_code"], "item_name": res["item_name"]}

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

        # 5. בניית התוצאות
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
