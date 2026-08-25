import os
import gzip
import re
import urllib.request
import psycopg2
from psycopg2.extras import execute_batch
from lxml import etree
import requests

DATABASE_URL = os.getenv("DATABASE_URL")

SCRAPE_TARGETS = [
    {
        "chain_id": "7290027600007",
        "chain_name": "שופרסל",
        "store_id": "001",
        "file_url": "http://prices.shufersal.co.il/FileServer/PriceFull/PriceFull7290027600007-001-latest.gz"
    },
    {
        "chain_id": "7290058140886",
        "chain_name": "רמי לוי",
        "store_id": "001",
        "file_url": "https://url.publishedprices.co.il/file/d/PriceFull7290058140886-001-latest.gz",
        "auth": ("RamiLevi", "")
    }
]

def clean_item_name(name: str) -> str:
    name = re.sub(r'[^\u0590-\u05FFa-zA-Z0-9\s%]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()

def download_and_parse_gz(target):
    headers = {"User-Agent": "Mozilla/5.0"}
    auth = target.get("auth")

    try:
        if auth:
            res = requests.get(target["file_url"], auth=auth, headers=headers, stream=True, timeout=60)
        else:
            res = requests.get(target["file_url"], headers=headers, stream=True, timeout=60)
            
        if res.status_code != 200:
            return []

        with gzip.GzipFile(fileobj=res.raw) as gz_file:
            context = etree.iterparse(gz_file, events=("end",), tag="Item")
            items_data = []
            for event, elem in context:
                try:
                    item_code = elem.findtext("ItemCode", "").strip()
                    raw_name = elem.findtext("ItemName", "").strip()
                    price_str = elem.findtext("ItemPrice", "0").strip()
                    manufacturer = elem.findtext("ManufacturerName", "").strip()

                    if item_code and raw_name and price_str:
                        items_data.append({
                            "chain_id": target["chain_id"],
                            "store_id": target["store_id"],
                            "item_code": item_code,
                            "item_name": clean_item_name(raw_name),
                            "manufacturer_name": manufacturer,
                            "item_price": float(price_str)
                        })
                finally:
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

            return items_data
    except Exception as e:
        print(f"Error scraping {target['chain_name']}: {e}")
        return []

def save_to_database(items_data):
    if not items_data or not DATABASE_URL:
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    products_batch = [(item["item_code"], item["item_name"], item["manufacturer_name"]) for item in items_data]
    prices_batch = [(item["chain_id"], item["store_id"], item["item_code"], item["item_price"]) for item in items_data]

    prod_query = """
    INSERT INTO products (item_code, item_name, manufacturer_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (item_code) DO UPDATE 
    SET item_name = EXCLUDED.item_name;
    """
    execute_batch(cur, prod_query, products_batch, page_size=2000)

    price_query = """
    INSERT INTO store_prices (chain_id, store_id, item_code, item_price, price_update_date)
    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (chain_id, store_id, item_code) DO UPDATE 
    SET item_price = EXCLUDED.item_price, price_update_date = CURRENT_TIMESTAMP;
    """
    execute_batch(cur, price_query, prices_batch, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()

def main():
    for target in SCRAPE_TARGETS:
        items = download_and_parse_gz(target)
        if items:
            save_to_database(items)

if __name__ == "__main__":
    main()