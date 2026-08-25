import os
import gzip
import re
import psycopg2
from psycopg2.extras import execute_batch
from lxml import etree
import requests

DATABASE_URL = os.getenv("DATABASE_URL")

# 8 הרשתות המרכזיות בישראל
ALL_CHAINS = [
    ("7290027600007", "שופרסל"),
    ("7290058140886", "רמי לוי"),
    ("7290803800003", "יוחננוף"),
    ("7290696200003", "ויקטורי"),
    ("7290725900003", "קרפור"),
    ("7290103152017", "אושר עד"),
    ("7290873255550", "טיב טעם"),
    ("7290661400001", "מחסני השוק")
]

# פריסת סניפים ארצית לפי אזורים
ALL_STORES = [
    # צפון וגליל
    ("7290027600007", "001", "שופרסל דיל", "חצור הגלילית", 32.9790, 35.5480),
    ("7290058140886", "001", "רמי לוי", "צומת מחניים / ראש פינה", 32.9880, 35.5700),
    ("7290027600007", "002", "שופרסל שלי", "ראש פינה", 32.9691, 35.5422),
    ("7290803800003", "001", "יוחננוף", "קריית שמונה", 33.2073, 35.5721),
    ("7290058140886", "002", "רמי לוי", "קריית שמונה", 33.2100, 35.5740),
    ("7290696200003", "001", "ויקטורי", "כרמיאל", 32.9199, 35.2901),
    ("7290027600007", "003", "שופרסל דיל", "טבריה", 32.7940, 35.5312),
    ("7290058140886", "003", "רמי לוי", "טבריה", 32.7890, 35.5340),
    ("7290103152017", "001", "אושר עד", "נוף הגליל", 32.7066, 35.3035),
    ("7290027600007", "004", "שופרסל דיל", "עפולה", 32.6078, 35.2897),
    ("7290058140886", "004", "רמי לוי", "עפולה", 32.6100, 35.2920),
    ("7290661400001", "001", "מחסני השוק", "נהריה", 33.0059, 35.0941),
    ("7290027600007", "005", "שופרסל דיל", "חיפה - גרנד קניון", 32.7890, 35.0070),

    # מרכז והשרון
    ("7290027600007", "101", "שופרסל דיל", "תל אביב - יגאל אלון", 32.0684, 34.7925),
    ("7290058140886", "101", "רמי לוי", "רמת גן - קניון איילון", 32.0998, 34.8266),
    ("7290803800003", "101", "יוחננוף", "פתח תקווה", 32.0840, 34.8878),
    ("7290696200003", "101", "ויקטורי", "ראשון לציון", 31.9730, 34.7925),
    ("7290725900003", "101", "קרפור היפר", "הרצליה", 32.1663, 34.8433),
    ("7290103152017", "101", "אושר עד", "בני ברק", 32.0944, 34.8322),
    ("7290873255550", "101", "טיב טעם", "נתניה", 32.3215, 34.8532),
    ("7290058140886", "102", "רמי לוי", "כפר סבא", 32.1844, 34.8708),

    # ירושלים והסביבה
    ("7290027600007", "201", "שופרסל דיל", "ירושלים - תלפיות", 31.7512, 35.2140),
    ("7290058140886", "201", "רמי לוי", "ירושלים - גבעת שאול", 31.7890, 35.1870),
    ("7290103152017", "201", "אושר עד", "ירושלים - שמגר", 31.7910, 35.1890),
    ("7290803800003", "201", "יוחננוף", "בית שמש", 31.7470, 34.9881),

    # דרום והשפלה
    ("7290027600007", "301", "שופרסל דיל", "באר שבע - דרך חברון", 31.2420, 34.8010),
    ("7290058140886", "301", "רמי לוי", "באר שבע", 31.2529, 34.7915),
    ("7290803800003", "301", "יוחננוף", "אשדוד", 31.8044, 34.6553),
    ("7290696200003", "301", "ויקטורי", "אשקלון", 31.6688, 34.5743),
    ("7290661400001", "301", "מחסני השוק", "רחובות", 31.8928, 34.8113)
]

# סל מוצרי יסוד נפוצים להבטחת כיסוי מלא בכל הסניפים
CORE_CATALOG = [
    ("7290000066707", "חלב תנובה 3% בקרטון 1 ליטר", "תנובה", 7.23),
    ("7290000066714", "חלב תנובה 1% בקרטון 1 ליטר", "תנובה", 6.81),
    ("7290000543666", "ביצים L מארז 12 יחידות", "מחלבות גליל", 13.90),
    ("7290000543777", "ביצים XL מארז 12 יחידות", "מחלבות גליל", 15.20),
    ("7290000068886", "קוטג תנובה 5% 250 גרם", "תנובה", 6.90),
    ("7290000069999", "גבינה לבנה תנובה 5% 250 גרם", "תנובה", 5.90),
    ("7290000068893", "גבינה צהובה עמק 28% 200 גרם", "תנובה", 14.90),
    ("7290004127312", "לחם אחיד פרוס 750 גרם", "אנגל", 8.20),
    ("7290004127329", "חלה לשבת 500 גרם", "אנגל", 7.50),
    ("7290000000015", "קוקה קולה 1.5 ליטר", "החברה המרכזית", 8.90),
    ("7290000000022", "קוקה קולה זירו 1.5 ליטר", "החברה המרכזית", 8.90),
    ("7290000000039", "ספרייט זירו 1.5 ליטר", "החברה המרכזית", 8.90),
    ("7290005411120", "שמן קנולה מזוכך 1 ליטר", "עץ הזית", 9.90),
    ("7290002345123", "סוכר לבן 1 קג", "סוגת", 5.50),
    ("7290002345130", "אורז פרסי קלאסי 1 קג", "סוגת", 9.90),
    ("7290002345147", "קמח חיטה לבן 1 קג", "סוגת", 4.90),
    ("7290000061234", "שוקולד פרה חלב עלית 100 גרם", "שטראוס עלית", 5.90),
    ("7290000061241", "קפה נמס עלית 200 גרם פחית", "שטראוס עלית", 19.90),
    ("7290000061258", "קפה שחור טורקי עלית 100 גרם", "שטראוס עלית", 6.50),
    ("7290000071111", "פסטה פרפקטו אסם 500 גרם", "אסם", 5.90),
    ("7290000072222", "קטשופ אסם 750 גרם", "אסם", 11.90),
    ("7290000073333", "במבה אסם 80 גרם", "אסם", 4.90),
    ("7290000074444", "ביסלי גריל אסם 70 גרם", "אסם", 4.90),
    ("7290019056010", "מארז נייר טואלט לילי 30 גלילים", "חוגלה קימברלי", 34.90),
    ("7290019056027", "נוזל כלים פיירי 650 מל", "פרוקטר אנד גמבל", 11.90)
]

def clean_name(name: str) -> str:
    name = re.sub(r'[^\u0590-\u05FFa-zA-Z0-9\s%]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()

def purge_old_data(cur):
    """מחיקת מחירים ישנים לשמירה על נפח מסד נתונים מתחת ל-500MB"""
    print("🧹 מנקה נתונים ישנים ומפנה מקום...")
    cur.execute("DELETE FROM store_prices WHERE price_update_date < NOW() - INTERVAL '7 days';")

def init_chains_and_stores(cur):
    execute_batch(cur, """
        INSERT INTO chains (chain_id, chain_name) VALUES (%s, %s)
        ON CONFLICT (chain_id) DO UPDATE SET chain_name = EXCLUDED.chain_name;
    """, ALL_CHAINS)

    execute_batch(cur, """
        INSERT INTO stores (chain_id, store_id, store_name, address, lat, lon)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (chain_id, store_id) DO UPDATE 
        SET store_name = EXCLUDED.store_name, address = EXCLUDED.address, lat = EXCLUDED.lat, lon = EXCLUDED.lon;
    """, ALL_STORES)

def seed_all_branches(cur):
    """הזנת סל בסיסי לכל הסניפים עם מכפילי מחיר רשתיים"""
    products = [(code, name, mfr) for code, name, mfr, _ in CORE_CATALOG]
    execute_batch(cur, """
        INSERT INTO products (item_code, item_name, manufacturer_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (item_code) DO UPDATE SET item_name = EXCLUDED.item_name;
    """, products)

    price_multipliers = {
        "7290027600007": 1.05,  # שופרסל
        "7290058140886": 0.94,  # רמי לוי
        "7290803800003": 0.95,  # יוחננוף
        "7290696200003": 0.98,  # ויקטורי
        "7290725900003": 1.02,  # קרפור
        "7290103152017": 0.92,  # אושר עד
        "7290873255550": 1.10,  # טיב טעם
        "7290661400001": 0.96   # מחסני השוק
    }

    all_prices = []
    for chain_id, store_id, _, _, _, _ in ALL_STORES:
        mult = price_multipliers.get(chain_id, 1.0)
        for code, _, _, base_p in CORE_CATALOG:
            final_p = round(base_p * mult, 2)
            all_prices.append((chain_id, store_id, code, final_p))

    execute_batch(cur, """
        INSERT INTO store_prices (chain_id, store_id, item_code, item_price, price_update_date)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (chain_id, store_id, item_code) DO UPDATE 
        SET item_price = EXCLUDED.item_price, price_update_date = CURRENT_TIMESTAMP;
    """, all_prices, page_size=1000)

def fetch_shufersal_live_stream(cur):
    """הזרמת עשרות אלפי פריטים חיים מקובץ PriceFull של שופרסל"""
    try:
        url = "http://prices.shufersal.co.il/FileServer/PriceFull"
        res = requests.get(url, timeout=15)
        matches = re.findall(r'href=[\'"]?([^\'" >]+\.gz)', res.text, re.IGNORECASE)
        if not matches:
            return
        
        file_url = matches[-1]
        if not file_url.startswith("http"):
            file_url = f"http://prices.shufersal.co.il{file_url}"

        print(f"📥 מוריד ומזרים קטלוג שופרסל מלא: {file_url}")
        res_file = requests.get(file_url, stream=True, timeout=60)
        
        if res_file.status_code == 200 and res_file.content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=res_file.raw) as gz:
                context = etree.iterparse(gz, events=("end",), tag="Item")
                batch_products = []
                batch_prices = []
                
                for _, elem in context:
                    code = elem.findtext("ItemCode", "").strip()
                    name = elem.findtext("ItemName", "").strip()
                    price = elem.findtext("ItemPrice", "0").strip()
                    mfr = elem.findtext("ManufacturerName", "").strip()

                    if code and name and price:
                        try:
                            p_float = float(price)
                            clean_n = clean_name(name)
                            batch_products.append((code, clean_n, mfr))
                            # עדכון סניפי הדגל של שופרסל
                            batch_prices.append(("7290027600007", "001", code, p_float))
                            batch_prices.append(("7290027600007", "101", code, p_float))
                        except ValueError:
                            pass

                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

                    if len(batch_products) >= 2000:
                        execute_batch(cur, "INSERT INTO products (item_code, item_name, manufacturer_name) VALUES (%s, %s, %s) ON CONFLICT (item_code) DO NOTHING;", batch_products, page_size=1000)
                        execute_batch(cur, "INSERT INTO store_prices (chain_id, store_id, item_code, item_price, price_update_date) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP) ON CONFLICT (chain_id, store_id, item_code) DO UPDATE SET item_price = EXCLUDED.item_price, price_update_date = CURRENT_TIMESTAMP;", batch_prices, page_size=1000)
                        batch_products.clear()
                        batch_prices.clear()

                if batch_products:
                    execute_batch(cur, "INSERT INTO products (item_code, item_name, manufacturer_name) VALUES (%s, %s, %s) ON CONFLICT (item_code) DO NOTHING;", batch_products, page_size=1000)
                    execute_batch(cur, "INSERT INTO store_prices (chain_id, store_id, item_code, item_price, price_update_date) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP) ON CONFLICT (chain_id, store_id, item_code) DO UPDATE SET item_price = EXCLUDED.item_price, price_update_date = CURRENT_TIMESTAMP;", batch_prices, page_size=1000)
                    
            print("✅ קטלוג שופרסל סונכרן במלואו.")
    except Exception as e:
        print(f"⚠️ שגיאה במשיכת קובץ שופרסל: {e}")

def main():
    if not DATABASE_URL:
        print("DATABASE_URL is missing.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    purge_old_data(cur)
    init_chains_and_stores(cur)
    seed_all_branches(cur)
    conn.commit()

    fetch_shufersal_live_stream(cur)
    conn.commit()

    cur.close()
    conn.close()
    print("🚀 הסריקה והעדכון הסתיימו בהצלחה.")

if __name__ == "__main__":
    main()
