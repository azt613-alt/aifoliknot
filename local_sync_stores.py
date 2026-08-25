import os
import io
import re
import gzip
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch
from requests.auth import HTTPBasicAuth
from lxml import etree

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
DATABASE_URL = os.getenv("DATABASE_URL")

SUPPORTED_CHAINS = [
    ("7290027600007", "שופרסל"),
    ("7290058140886", "רמי לוי"),
    ("7290803800003", "יוחננוף"),
    ("7290103152017", "אושר עד"),
    ("7290696200003", "ויקטורי"),
    ("7290725900003", "קרפור"),
    ("7290873255550", "טיב טעם"),
    ("7290661400001", "מחסני השוק")
]

CHAIN_PORTALS = [
    ("7290027600007", "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID=5&sort=Time&sortdir=DESC", None),
    ("7290058140886", "https://url.publishedprices.co.il/file/d/RamiLevi", HTTPBasicAuth("RamiLevi", "")),
    ("7290803800003", "https://url.publishedprices.co.il/file/d/yohananof", HTTPBasicAuth("yohananof", "")),
    ("7290103152017", "https://url.publishedprices.co.il/file/d/OsherAd", HTTPBasicAuth("OsherAd", "")),
    ("7290696200003", "https://url.publishedprices.co.il/file/d/Victory", HTTPBasicAuth("Victory", "")),
    ("7290725900003", "https://url.publishedprices.co.il/file/d/Mega", HTTPBasicAuth("Mega", "")),
    ("7290873255550", "https://url.publishedprices.co.il/file/d/TivTaam", HTTPBasicAuth("TivTaam", "")),
    ("7290661400001", "https://url.publishedprices.co.il/file/d/Coop", HTTPBasicAuth("Coop", ""))
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

GEO_REGISTRY = {
    "ראש פינה": (32.9691, 35.5422), "חצור הגלילית": (32.9790, 35.5480), "חצור": (32.9790, 35.5480),
    "מחניים": (32.9860, 35.5680), "צפת": (32.9646, 35.4960), "קריית שמונה": (33.2073, 35.5721),
    "קרית שמונה": (33.2073, 35.5721), "קצרין": (32.9920, 35.6880), "טבריה": (32.7940, 35.5312),
    "כרמיאל": (32.9199, 35.2901), "מעלות תרשיחא": (33.0160, 35.2750), "מעלות": (33.0160, 35.2750),
    "נהריה": (33.0059, 35.0941), "שלומי": (33.0730, 35.1430), "עכו": (32.9278, 35.0818),
    "עפולה": (32.6078, 35.2897), "נוף הגליל": (32.7066, 35.3035), "נצרת עילית": (32.7066, 35.3035),
    "נצרת": (32.6996, 35.3035), "מגדל העמק": (32.6730, 35.2400), "בית שאן": (32.4970, 35.4980),
    "יקנעם": (32.6590, 35.0810), "יקנעם עילית": (32.6590, 35.0810), "חיפה": (32.7940, 34.9896),
    "נשר": (32.7750, 35.0350), "טירת כרמל": (32.7600, 34.9700), "קרית אתא": (32.8020, 35.1050),
    "קרית ביאליק": (32.8320, 35.0800), "קרית מוצקין": (32.8380, 35.0780), "קרית ים": (32.8450, 35.0680),
    "קרית טבעון": (32.7150, 35.1250), "עתלית": (32.6880, 34.9350), "חדרה": (32.4340, 34.9190),
    "אור עקיבא": (32.5060, 34.9180), "זכרון יעקב": (32.5730, 34.9530), "בנימינה": (32.5180, 34.9500),
    "פרדס חנה כרכור": (32.4710, 34.9720), "חריש": (32.4600, 35.0400), "נתניה": (32.3215, 34.8532),
    "כפר יונה": (32.3160, 34.9350), "כפר סבא": (32.1844, 34.8708), "רעננה": (32.1840, 34.8710),
    "הוד השרון": (32.1550, 34.8880), "הרצליה": (32.1663, 34.8433), "רמת השרון": (32.1480, 34.8390),
    "תל אביב - יפו": (32.0853, 34.7818), "תל אביב": (32.0853, 34.7818), "רמת גן": (32.0684, 34.8248),
    "גבעתיים": (32.0720, 34.8100), "בני ברק": (32.0944, 34.8322), "פתח תקווה": (32.0840, 34.8878),
    "גבעת שמואל": (32.0780, 34.8480), "קרית אונו": (32.0630, 34.8580), "חולון": (32.0158, 34.7874),
    "בת ים": (32.0200, 34.7500), "ראשון לציון": (31.9730, 34.7925), "נס ציונה": (31.9300, 34.7990),
    "רחובות": (31.8928, 34.8113), "באר יעקב": (31.9380, 34.8350), "רמלה": (31.9270, 34.8640),
    "לוד": (31.9520, 34.8970), "שוהם": (31.9980, 34.9450), "מודיעין מכבים רעות": (31.8903, 35.0104),
    "מודיעין": (31.8903, 35.0104), "ירושלים": (31.7683, 35.2137), "מבשרת ציון": (31.7997, 35.1542),
    "מעלה אדומים": (31.7921, 35.2974), "בית שמש": (31.7470, 34.9881), "ביתר עילית": (31.6980, 35.1150),
    "יבנה": (31.8767, 34.7408), "גדרה": (31.8130, 34.7780), "גן יבנה": (31.7880, 34.7150), "קרית עקרון": (31.8600, 34.8200),
    "אשדוד": (31.8044, 34.6553), "אשקלון": (31.6688, 34.5743), "קרית גת": (31.6100, 34.7640),
    "קרית מלאכי": (31.7280, 34.7450), "שדרות": (31.5215, 34.5959), "נתיבות": (31.4200, 34.5800),
    "אופקים": (31.3140, 34.6200), "באר שבע": (31.2529, 34.7915), "דימונה": (31.0700, 35.0300),
    "ערד": (31.2610, 35.2140), "אילת": (29.5577, 34.9519)
}

def resolve_coords(store_name: str, address: str, city: str):
    combined = f"{city} {address} {store_name}".strip()
    if city and city in GEO_REGISTRY:
        return GEO_REGISTRY[city]
    for place, coords in GEO_REGISTRY.items():
        if place in combined:
            return coords
    return 32.0853, 34.7818

def parse_stores_xml_content(content: bytes, chain_id: str):
    stores = []
    if content.lstrip().startswith(b'<!DOCTYPE') or content.lstrip().startswith(b'<html') or content.lstrip().startswith(b'<HTML'):
        return stores

    try:
        is_gz = content[:2] == b'\x1f\x8b'
        file_obj = gzip.GzipFile(fileobj=io.BytesIO(content)) if is_gz else io.BytesIO(content)

        context = etree.iterparse(file_obj, events=('end',), tag=['Store', 'STORE', 'store', 'Branch', 'BRANCH'])
        for _, elem in context:
            s_id = None
            s_name = None
            address = ""
            city = ""

            for child in elem:
                tag = child.tag.lower().split('}')[-1]
                val = (child.text or "").strip()
                if not val:
                    continue
                if tag in ['storeid', 'branchid', 'id']:
                    s_id = val
                elif tag in ['storename', 'branchname', 'name']:
                    s_name = val
                elif tag in ['address', 'street', 'storeaddress']:
                    address = val
                elif tag in ['city', 'cityname']:
                    city = val

            if s_id:
                name = s_name or f"סניף {s_id}"
                lat, lon = resolve_coords(name, address, city)
                full_addr = f"{address}, {city}".strip(", ") if city and city not in address else (address or city)
                stores.append((chain_id, s_id, name, full_addr, lat, lon))

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        del context
    except Exception as e:
        logging.error(f"שגיאה בפענוח XML: {e}")
    return stores

def main():
    if not DATABASE_URL:
        logging.error("DATABASE_URL חסר.")
        return

    logging.info("מתחבר למסד הנתונים ב-Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chains (chain_id TEXT PRIMARY KEY, chain_name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stores (
                chain_id TEXT NOT NULL, store_id TEXT NOT NULL, store_name TEXT NOT NULL,
                address TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                PRIMARY KEY (chain_id, store_id)
            );
            CREATE TABLE IF NOT EXISTS products (item_code TEXT PRIMARY KEY, item_name TEXT NOT NULL, manufacturer_name TEXT);
            CREATE TABLE IF NOT EXISTS store_prices (
                chain_id TEXT NOT NULL, store_id TEXT NOT NULL, item_code TEXT NOT NULL,
                item_price NUMERIC(10, 2) NOT NULL, price_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chain_id, store_id, item_code)
            );
            CREATE INDEX IF NOT EXISTS idx_store_prices_lookup ON store_prices(item_code, chain_id, store_id);
            CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products(item_name);
        """)
        execute_batch(cur, "INSERT INTO chains (chain_id, chain_name) VALUES (%s, %s) ON CONFLICT (chain_id) DO NOTHING;", SUPPORTED_CHAINS)
        conn.commit()

    all_official_stores = []

    for chain_id, portal_url, auth in CHAIN_PORTALS:
        session = requests.Session()
        session.headers.update(HEADERS)
        if auth:
            session.auth = auth

        try:
            logging.info(f"סורק פורטל עבור רשת {chain_id}...")
            r = session.get(portal_url, timeout=35)
            if r.status_code == 200:
                # חיפוש כל קישור או קובץ שמכיל את המילים store או stores (באותיות קטנות או גדולות)
                matches = re.findall(r'href=[\'"]([^\'"]*(?:store|stores)[^\'"]*)[\'"]', r.text, re.IGNORECASE)
                if not matches:
                    matches = re.findall(r'([^\'"<>]*?(?:store|stores)[^\'"<>]*\.(?:gz|xml))', r.text, re.IGNORECASE)

                logging.info(f"רשת {chain_id}: נמצאו {len(matches)} התאמות לקובצי סניפים")

                for m in set(matches):
                    m_clean = m.strip().split('"')[0].split("'")[0]
                    if m_clean.startswith("http"):
                        file_url = m_clean
                    elif m_clean.startswith("/"):
                        file_url = f"https://url.publishedprices.co.il{m_clean}" if auth else f"http://prices.shufersal.co.il{m_clean}"
                    else:
                        username = auth.username if auth else ""
                        file_url = f"https://url.publishedprices.co.il/file/d/{username}/download?file={m_clean}" if username else f"http://prices.shufersal.co.il/{m_clean}"

                    f_resp = session.get(file_url, timeout=35)
                    if f_resp.status_code == 200:
                        parsed = parse_stores_xml_content(f_resp.content, chain_id)
                        if parsed:
                            all_official_stores.extend(parsed)
                            logging.info(f"רשת {chain_id}: נפרקו בהצלחה {len(parsed)} סניפים")
                            break
            else:
                logging.warning(f"רשת {chain_id}: שגיאת HTTP {r.status_code}")
        except Exception as e:
            logging.error(f"שגיאה בעיבוד רשת {chain_id}: {e}")

    unique_stores = list({(s[0], s[1]): s for s in all_official_stores}.values())
    logging.info(f"🎯 סה\"כ חולצו {len(unique_stores)} סניפי אמת פעילים מכל הרשתות!")

    if unique_stores:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM store_prices;")
            cur.execute("DELETE FROM stores;")
            query = """
                INSERT INTO stores (chain_id, store_id, store_name, address, lat, lon)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (chain_id, store_id) DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon;
            """
            execute_batch(cur, query, unique_stores, page_size=1000)
            conn.commit()
        logging.info("✅ כל סניפי האמת הרשמיים נרשמו בהצלחה ב-Supabase!")

    conn.close()

if __name__ == "__main__":
    main()
