import requests
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys
import os
from dotenv import load_dotenv
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import math
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data


BASE_URL = "https://www.prettylittlething.com"
load_dotenv()

proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None


def clean_and_save_product_data_only_available_with_all_images_from_data(
    data, gender_tag=None, product_type=None
):
    # Extract products from the JSON structure (new structure assumes 'products' at root)
    # Handle both dict with 'products' key and direct list of products
    if isinstance(data, list):
        products = data
    else:
        products = data.get("products", [])
    cleaned_products = {}

    for product in products:
        if product is None:
            continue

        # Check if the product is in stock (assuming 'in_stock' field exists, default to "true" if missing)
        if product.get("in_stock", "true") != "true":
            continue

        # Use 'pid' as handle if 'url' is not present, or adapt based on available fields
        handle = product.get("url", product.get("pid", "")).replace(".html", "") if product.get("url") else product.get("pid", "")
        title = product.get("title", product.get("product_name_en", ""))
        description = f"<p>{product.get('product_name_en', product.get('title', ''))}</p>"  # Fallback to title if product_name_en missing
        brand = product.get("brand", "PrettyLittleThing")  # Default to PrettyLittleThing if brand missing
        product_tags = product.get("colors", [])  # Using colors as tags, assuming it exists
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}

        type_val = product.get("sub_category", product.get("style", ""))  # Fallback to style if style_en missing
        category_val = product.get("category_en", product.get("category", ""))  # Fallback to category if category_en missing
        all_tags = product_tags + list(gender_tags)
        if type_val:
            all_tags.extend(type_val.split())
        if category_val:
            all_tags.append(category_val)
        product_tags = ", ".join(tag.strip() for tag in set(all_tags) if tag.strip())

        # Collect image URL (assuming thumb_image exists, else empty list)
        all_images = [product.get("thumb_image", "")] if product.get("thumb_image") else []

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": gender_tag.lower() if gender_tag else category_val,
                "Type": type_val,
                "Tags": product_tags,
                "variants": []
            }

        seen = set()
        # Handle variants (assuming 'variants' field exists, else treat product as a single variant)
        variants = product.get("variants", [])
        if not variants:  # If no variants, treat the product itself as a single variant
            sku = product.get("pid", "")
            size = product.get("clothes_size", "")
            color = product.get("colour_en", product.get("colors", [""])[0] if product.get("colors") else "")
            price = float(product.get("sale_price", product.get("slider_price", 0)))
            compare_price = float(product.get("price", 0)) if product.get("price") else 0

            if (size, sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": price,
                    "Variant Compare At Price": compare_price,
                    "images": all_images
                })
                seen.add((size, sku))
        else:
            for variant in variants:
                # Check if the variant is in stock
                if variant.get("in_stock", ["true"])[0] != "true":
                    continue

                sku = variant.get("skuid", product.get("pid", ""))
                size = variant.get("clothes_size", [""])[0]
                color = product.get("colour_en", product.get("colors", [""])[0] if product.get("colors") else "")
                price = float(product.get("sale_price", product.get("slider_price", 0)))
                compare_price = float(product.get("price", 0)) if product.get("price") else 0

                if (size, sku) not in seen:
                    cleaned_products[handle]["variants"].append({
                        "Variant SKU": sku,
                        "size": size,
                        "color": color,
                        "Variant Price": price,
                        "Variant Compare At Price": compare_price,
                        "images": all_images
                    })
                    seen.add((size, sku))

    # Return as a list of product dicts
    return list(cleaned_products.values())

# Function to read JSON file, process it, and save the cleaned data
def process_and_save_json(data, output_file, gender_tag=None, product_type=None):

    # Process the data
    cleaned_data = clean_and_save_product_data_only_available_with_all_images_from_data(data, gender_tag, product_type)

    upsert_all_product_data(cleaned_data, BASE_URL, "USD")
    # Save the cleaned data to a new JSON file
    with open(output_file, 'w') as file:
        json.dump(cleaned_data, file, indent=4)
    
    print(f"Cleaned data has been saved to {output_file}")
    



def fetch_page(base_url, start, rows=48):
    """
    Fetch a single page of products from the PrettyLittleThing API.
    Args:
        base_url (str): The base API URL without the start parameter.
        start (int): Starting index for pagination.
        rows (int): Number of products per page.
    Returns:
        tuple: (start, products, total_products) where products is the list of fetched products,
               total_products is the total count from the API (if available), or None if an error occurs.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.prettylittlething.com/sale.html',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    url = f"{base_url}&start={start}"
    print(f"Fetching page starting at index {start}...")

    try:
        response = session.get(url, headers=headers, proxies=proxies, timeout=10)
        response.raise_for_status()
        data = response.json()
        products = data.get("response", {}).get("docs", [])
        total_products = data.get("response", {}).get("numFound", None)
        print(f"Fetched {len(products)} products from start={start}")
        return start, products, total_products
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching page at start={start}: {e}")
        return start, None, None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page at start={start}: {e}")
        return start, None, None
    except json.JSONDecodeError:
        print(f"Error decoding JSON at start={start}")
        return start, None, None

def fetch_all_products(base_url, rows=48, max_workers=10):
    """
    Fetch all products using threading with rotating proxies.
    Args:
        base_url (str): The base API URL with all parameters except start.
        rows (int): Number of products per page (default is 48).
        max_workers (int): Maximum number of threads (default is 10).
    Returns:
        list: A list of all products fetched from the API.
    """
    all_products = []
    start_queue = Queue()
    lock = threading.Lock()

    # Fetch first page to get total products
    start, products, total_products = fetch_page(base_url, 0, 48)
    if not products:
        print("Failed to fetch initial page.")
        return all_products
    with lock:
        all_products.extend(products)
    #return all_products

    # Use total_products from API response, default to 48 if not available
    total_products = total_products if total_products is not None else rows
    # total_products = 200
    print(f"Total products to fetch: {total_products}")

    # Populate queue with start indices for remaining pages
    start = rows
    while start < total_products:
        start_queue.put(start)
        start += rows

    def worker():
        while not start_queue.empty():
            start_index = start_queue.get()
            start, products, _ = fetch_page(base_url, start_index, rows)
            if products:
                with lock:
                    all_products.extend(products)
            else:
                print(f"No products returned for start={start_index}, stopping further fetches for this thread.")
            start_queue.task_done()
    # Use ThreadPoolExecutor for concurrent fetching
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _ in range(min(max_workers, start_queue.qsize())):
            executor.submit(worker)

    start_queue.join()  # Wait for all tasks to complete
    print(f"Total products fetched: {len(all_products)}")
    return all_products

def save_products_to_json(products, output_file):
    """
    Save the fetched products to a JSON file.
    Args:
        products (list): List of products to save.
        output_file (str): Path to the output JSON file.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({"products": products}, f, indent=4)
        print(f"Saved {len(products)} products to {output_file}")
    except Exception as e:
        print(f"Error saving to {output_file}: {e}")


def complete_workflow_pretty_little_things():

    # Base URL from the provided request (without the start parameter)
    base_url = (
        "https://www.prettylittlething.com/br/core/api/v1/core/?fl=pid%2Ctitle%2Cis_multi_colour%2Cprice%2Csale_price%2Cthumb_image%2Curl%2Csticker_path%2Ccategory_en%2Ccolour_en%2Ccolors%2Cstyle_en%2Cstyle%2Csub_category%2Cbrand%2Cproduct_name_en%2Cspecial_price%2Cspecial_price_from_date%2Cspecial_price_to_date%2Cspecial_price_set_b%2Cspecial_price_from_date_set_b%2Cspecial_price_to_date_set_b%2Cslider_price%2Cskuid%2Cclothes_size%2Cshoesize%2Cbra_size%2Caccessory_size%2Cbelt_size%2Cin_stock%2Creporting_category&_br_uid_2=uid%3D4623219268953%3Av%3D17.0%3Ats%3D1754318991453%3Ahc%3D9%3Acdp_segments%3DNjYzYTVmYzE4NGZhMzY2Y2EwZjRlNTRjOjY2M2NmYzU5ZjdmZjM1NmJjNTI2YmM3NCw2NjNjYWY5YjI1Y2M5ODQwYTU0MDkzMjM6NjYzY2FmOWIyNWNjOTg0MGE1NDA5MmRkLDY2ZWM1MDI5MmM0YjYyY2I1YzA0ZDQxZDo2NmVjNTNmYmRjMGViMTM4MGIzY2UwODcsNjZmNDIyNjQxOTBmYjQ1OTU0MjQxODE5OjY2ZjQyMjY0MTkwZmI0NTk1NDI0MTgwNA%3D%3D&search_type=category&rows=48&request_id=4250147799838&account_id=7513&domain_key=prettylittlething&q=39&request_type=search&url=https%3A%2F%2Fwww.prettylittlething.com%2Fsale.html%3Fstyle%3DSweatshirt%252CBodycon%2BDress%252CShift%2BDress%252CMini%2BSkirt%252CVest%2BTop%252CCami%2BTop%252CMaxi%2BSkirt%252CWide%2BLeg%2BTrousers%252CStraight%2BLeg%2BTrousers%252CShirt%252CBardot%2BTop%252CBandeau%2BTop%252CWide%2BLeg%2BJoggers%252CT-Shirt%252CCorset%252CFitted%2BT-Shirt%252CWaistcoat%252CBlouse%252CFloaty%2BShorts%252CBlazer%2BDress%252CA-line%2B%252F%2BSkater%2BDress%252CFitted%2BBlazer%252CStraight%2BLeg%2BJoggers%252CCardigan%252CCuffed%2BJoggers%252CSandals%252CJean%252CBeach%2BSkirt%252CCorset%2BDress%252CBeach%2BTop%252CJumper%2BDress%252CKnitted%2BTrousers%252CMidi%2BSkirt%252COversized%2BT-Shirt%252CKnitted%2BSkirt%252COversized%2BJumper%252CSplit%2BHem%2BJeans%252CDenim%2BMini%2BSkirt%252CSlip%2BDress%252CMidaxi%2BSkirt%252CDenim%2BShorts%252CFlared%2BJeans%252CBomber%2BJacket%252CCropped%2BDenim%2BJacket%252CSkinny%2BJeans&ref_url=https%3A%2F%2Fwww.google.com%2F&stats.field=slider_price&fq=available_stock%3A+%5B1+TO+*%5D&fq=style%3A%22Sweatshirt%22+OR+%22Bodycon+Dress%22+OR+%22Shift+Dress%22+OR+%22Mini+Skirt%22+OR+%22Vest+Top%22+OR+%22Cami+Top%22+OR+%22Maxi+Skirt%22+OR+%22Wide+Leg+Trousers%22+OR+%22Straight+Leg+Trousers%22+OR+%22Shirt%22+OR+%22Bardot+Top%22+OR+%22Bandeau+Top%22+OR+%22Wide+Leg+Joggers%22+OR+%22T-Shirt%22+OR+%22Corset%22+OR+%22Fitted+T-Shirt%22+OR+%22Waistcoat%22+OR+%22Blouse%22+OR+%22Floaty+Shorts%22+OR+%22Blazer+Dress%22+OR+%22A-line+%2F+Skater+Dress%22+OR+%22Fitted+Blazer%22+OR+%22Straight+Leg+Joggers%22+OR+%22Cardigan%22+OR+%22Cuffed+Joggers%22+OR+%22Sandals%22+OR+%22Jean%22+OR+%22Beach+Skirt%22+OR+%22Corset+Dress%22+OR+%22Beach+Top%22+OR+%22Jumper+Dress%22+OR+%22Knitted+Trousers%22+OR+%22Midi+Skirt%22+OR+%22Oversized+T-Shirt%22+OR+%22Knitted+Skirt%22+OR+%22Oversized+Jumper%22+OR+%22Split+Hem+Jeans%22+OR+%22Denim+Mini+Skirt%22+OR+%22Slip+Dress%22+OR+%22Midaxi+Skirt%22+OR+%22Denim+Shorts%22+OR+%22Flared+Jeans%22+OR+%22Bomber+Jacket%22+OR+%22Cropped+Denim+Jacket%22+OR+%22Skinny+Jeans%22"
    )

    # Fetch all products with threading and proxies
    products = fetch_all_products(base_url, rows=48, max_workers=40)
    process_and_save_json(products, "clean_products.json", gender_tag='women', product_type=None)
    # Save products to JSON file
    output_file = "all_products.json"
    save_products_to_json(products, output_file)


if __name__ == "__main__":
    complete_workflow_pretty_little_things()