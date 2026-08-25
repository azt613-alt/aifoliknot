def fetch_shufersal_stores_files():
    """מושך את כל קובצי הסניפים של שופרסל על כל תתי-הרשתות שלה"""
    files = []
    try:
        url = CHAIN_CONFIGS["7290027600007"]["stores_url"]
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            matches = re.findall(r'href=[\'"]([^\'"]*Stores(?:Full)?[^\'"]*\.gz)[\'"]', r.text, re.IGNORECASE)
            # לוקח את כל קובצי הסניפים הייחודיים
            files = list(set(matches))
    except Exception as e:
        logging.warning(f"שופרסל: לא אותרו קובצי StoresFull ({e})")
    return files

def fetch_cerberus_stores_file(portal_url: str, auth):
    """מאתר תמיד את קובץ ה-StoresFull המלא והעדכני ביותר ברשתות Cerberus"""
    try:
        r = requests.get(portal_url, headers=HEADERS, auth=auth, timeout=25)
        if r.status_code == 200:
            # עדיפות ראשונה לקובץ StoresFull מלא
            full_matches = re.findall(r'href=[\'"]([^\'"]*StoresFull[^\'"]*(?:\.gz|\.xml))[\'"]', r.text, re.IGNORECASE)
            if full_matches:
                # לוקח את הקובץ האחרון ברשימה (העדכני ביותר)
                target = full_matches[-1]
                return target if target.startswith("http") else f"https://url.publishedprices.co.il{target}"
            
            # עדיפות משנית לקובץ Stores רגיל
            matches = re.findall(r'href=[\'"]([^\'"]*Stores[^\'"]*(?:\.gz|\.xml))[\'"]', r.text, re.IGNORECASE)
            if matches:
                target = matches[-1]
                return target if target.startswith("http") else f"https://url.publishedprices.co.il{target}"
    except Exception as e:
        logging.warning(f"Cerberus ({portal_url}): תקלה בשליפת קובץ סניפים ({e})")
    return None

def sync_official_stores(conn):
    """טוען את כל הסניפים הפעילים מכל הקבצים המלאים"""
    logging.info("🏢 מתחיל סנכרון של כלל סניפי האמת מקובצי StoresFull...")
    all_official_stores = []

    # 1. שופרסל - מעבר על כלל תתי הרשתות
    shuf_stores_files = fetch_shufersal_stores_files()
    for s_url in shuf_stores_files:
        stores = parse_stores_xml_stream(s_url, None, "7290027600007")
        all_official_stores.extend(stores)

    # 2. רשתות Cerberus (רמי לוי, יוחננוף, אושר עד, ויקטורי, קרפור, טיב טעם, מחסני השוק)
    for c_id, cfg in CHAIN_CONFIGS.items():
        if c_id == "7290027600007":
            continue
        stores_url = fetch_cerberus_stores_file(cfg["portal_url"], cfg["auth"])
        if stores_url:
            stores = parse_stores_xml_stream(stores_url, cfg["auth"], c_id)
            all_official_stores.extend(stores)

    if all_official_stores:
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
            execute_batch(cur, query, all_official_stores, page_size=1000)
            conn.commit()
        logging.info(f"✨ נטענו בהצלחה {len(all_official_stores)} סניפים פעילים!")
