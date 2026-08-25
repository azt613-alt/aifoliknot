import os
import gzip
import re
import psycopg2
from psycopg2.extras import execute_batch
from lxml import etree
import requests

DATABASE_URL = os.getenv("DATABASE_URL")

INITIAL_CHAINS_AND_STORES = {
    "chains": [
        ("7290027600007", "שופרסל"),
        ("7290058140886", "רמי לוי")
    ],
    "stores": [
        ("7290027600007", "001", "שופרסל דיל גליל עליון", "חצור הגלילית", 32.9790, 35.5480),
        ("7290058140886", "001", "רמי לוי מחניים", "צומת מחניים", 32.9880, 35.5700)
    ]
}

def clean_item_name(name: str) -> str:
    name = re.sub(r'[^\u0590-\u05FFa-zA-Z0-9\s%]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()

def init_base_data(cur):
    execute_batch(cur, """
        INSERT INTO chains (chain_id, chain_name) VALUES (%s, %s)
        ON CONFLICT (chain_id) DO NOTHING;
    """, INITIAL_CHAINS_AND_STORES["chains"])
    
    execute_batch(cur, """
        INSERT INTO stores (chain_id, store_id, store_name, address, lat, lon) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (chain_id, store_id) DO UPDATE 
        SET store_name = EXCLUDED.store_name, address = EXCLUDED.address, lat = EXCLUDED.lat, lon = EXCLUDED.lon;
    """, INITIAL_CHAINS_AND_STORES["stores"])

def fetch_shufersal_latest_url():
    """איתור קובץ PriceFull העדכני מפורטל שופרסל"""
    try:
        url = "http://prices.shufersal.co.il/FileServer/PriceFull"
        res = requests.get(url, timeout=15)
        matches = re.findall(r'href=[\'"]?([^\'" >]+\.gz)', res.text, re.IGNORECASE)
        if matches:
            latest_file = matches[-1]
            if not latest_file.startswith("http"):
                return f"http://prices.shufersal.co.il{latest_file}"
            return latest_file
    except Exception as e:
        print(f"Error resolving Shufersal URL: {e}")
    return None

def fetch_and_process_gz(file_url, chain_id, store_id, chain_name, auth=None):
    print(f"📥 מוריד קובץ מחירים מ-{chain_name}...")
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        res = requests.get(file_url, auth=auth, headers=headers, stream=True, timeout=60)
        if res.status_code != 200 or not res.content.startswith(b'\x1f\x8b'):
            print(f"⚠️ הקובץ מ-{chain_name} אינו קובץ GZ תקין.")
            return []

        with gzip.GzipFile(fileobj=res.raw) as gz_file:
            context = etree.iterparse(gz_file, events=("end",), tag="Item")
            items = []
            for event, elem in context:
                try:
                    code = elem.findtext("ItemCode", "").strip()
                    name = elem.findtext("ItemName", "").strip()
                    price = elem.findtext("ItemPrice", "0").strip()
                    mfr = elem.findtext("ManufacturerName", "").strip()

                    if code and name and price:
                        items.append((code, clean_item_name(name), mfr, chain_id, store_id, float(price)))
                finally:
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
            return items
    except Exception as e:
        print(f"❌ שגיאה בעיבוד {chain_name}: {e}")
        return []

def seed_sample_catalog(cur):
    """הזנת סל בסיסי של מוצרי יסוד נפוצים להפעלה מיידית"""
    sample_products = [
        ("7290000066707", "חלב תנובה 3% בקרטון 1 ליטר", "תנובה"),
        ("7290000066714", "חלב תנובה 1% בקרטון 1 ליטר", "תנובה"),
        ("7290000543666", "ביצים L מארז 12 יחידות", "מחלבות גליל"),
        ("7290000068886", "קוטג תנובה 5% 250 גרם", "תנובה"),
        ("7290000069999", "גבינה לבנה תנובה 5% 250 גרם", "תנובה"),
        ("7290004127312", "לחם אחיד פרוס 750 גרם", "אנגל"),
        ("7290000000015", "קוקה קולה 1.5 ליטר", "החברה המרכזית"),
        ("7290000000022", "קוקה קולה זירו 1.5 ליטר", "החברה המרכזית"),
        ("7290005411120", "שמן קנולה 1 ליטר מזוכך", "עץ הזית"),
        ("7290002345123", "סוכר לבן 1 קג", "סוגת")
    ]
    
    sample_prices = [
        # שופרסל
        ("7290027600007", "001", "7290000066707", 7.23),
        ("7290027600007", "001", "7290000066714", 6.81),
        ("7290027600007", "001", "7290000543666", 13.90),
        ("7290027600007", "001", "7290000068886", 6.90),
        ("7290027600007", "001", "7290000069999", 5.90),
        ("7290027600007", "001", "7290004127312", 8.20),
        ("7290027600007", "001", "7290000000015", 8.90),
        ("7290027600007", "001", "7290000000022", 8.90),
        ("7290027600007", "001", "7290005411120", 9.90),
        ("7290027600007", "001", "7290002345123", 5.50),
        # רמי לוי
        ("7290058140886", "001", "7290000066707", 7.23),
        ("7290058140886", "001", "7290000066714", 6.81),
        ("7290058140886", "001", "7290000543666", 13.30),
        ("7290058140886", "001", "7290000068886", 6.20),
        ("7290058140886", "001", "7290000069999", 5.40),
        ("7290058140886", "001", "7290004127312", 7.90),
        ("7290058140886", "001", "7290000000015", 7.90),
        ("7290058140886", "001", "7290000000022", 7.90),
        ("7290058140886", "001", "7290005411120", 8.90),
        ("7290058140886", "001", "7290002345123", 4.90)
    ]

    execute_batch(cur, """
        INSERT INTO products (item_code, item_name, manufacturer_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (item_code) DO UPDATE SET item_name = EXCLUDED.item_name;
    """, sample_products)

    execute_batch(cur, """
        INSERT INTO store_prices (chain_id, store_id, item_code, item_price, price_update_date)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (chain_id, store_id, item_code) DO UPDATE 
        SET item_price = EXCLUDED.item_price, price_update_date = CURRENT_TIMESTAMP;
    """, sample_prices)

def main():
    if not DATABASE_URL:
        print("DATABASE_URL not found.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("🌱 מזין סניפים ומוצרי בסיס...")
    init_base_data(cur)
    seed_sample_catalog(cur)
    conn.commit()

    # סריקת שופרסל דינמית
    shufersal_url = fetch_shufersal_latest_url()
    if shufersal_url:
        items = fetch_and_process_gz(shufersal_url, "7290027600007", "001", "שופרסל")
        if items:
            prod_batch = [(x[0], x[1], x[2]) for x in items]
            price_batch = [(x[3], x[4], x[0], x[5]) for x in items]
            execute_batch(cur, "INSERT INTO products (item_code, item_name, manufacturer_name) VALUES (%s, %s, %s) ON CONFLICT (item_code) DO NOTHING;", prod_batch, page_size=2000)
            execute_batch(cur, "INSERT INTO store_prices (chain_id, store_id, item_code, item_price) VALUES (%s, %s, %s, %s) ON CONFLICT (chain_id, store_id, item_code) DO UPDATE SET item_price = EXCLUDED.item_price;", price_batch, page_size=2000)
            conn.commit()
            print(f"✅ שופרסל: עודכנו {len(items)} מוצרים.")

    cur.close()
    conn.close()
    print("🚀 תהליך העדכון הסתיים בהצלחה.")

if __name__ == "__main__":
    main()
