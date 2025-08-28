import requests
import math
import json
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import logging
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

# ----------------- Setup -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None
BASE_URL="https://www.riverisland.com"

graphql_url = "https://api-v2.riverisland.com/graphql"

variables = {
    "getFacets": True,
    "isBrowser": True,
    "keyword": "",
    "path": "women",
    "page": 1,
    "pageSize": 40,
    "refinementsQuery": (
        "f-cat=coats--jackets&"
        "f-cat=dresses&"
        "f-cat=jeans&"
        "f-cat=knitwear&"
        "f-cat=playsuits--jumpsuits&"
        "f-cat=shorts&"
        "f-cat=skirts&"
        "f-cat=swimwear-and-beachwear&"
        "f-cat=tops&"
        "f-cat=trousers"
    ),
    "sort": "latest",
    "isSearch": False,
    "host": "www.riverisland.com",
    "countryCode": "US",
    "currencyCode": "USD",
    "clusterId": 0
}

extensions = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "c54537610d9304cc8c5177e82c5676e64dbaf2bce31d43d297a5ae853331fa0f"
    }
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36"
}

# ----------------- Step 1: Fetch listing pages -----------------
def fetch_page(page: int):
    variables["page"] = page
    try:
        resp = requests.get(
            graphql_url,
            params={
                "operationName": "ProductsAndFacets",
                "variables": json.dumps(variables),
                "extensions": json.dumps(extensions)
            },
            headers=headers,
            proxies=proxies,
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()["data"]["productsAndFacets"]["listProducts"]
        return ["https://www.riverisland.com" + p["productPageUrl"] for p in data]
    except Exception as e:
        logger.warning(f"❌ Error fetching page {page}: {e}")
        return []


def get_all_product_urls():
    # Get total count from first page
    first = fetch_page(1)
    total_count = requests.get(
        graphql_url,
        params={
            "operationName": "ProductsAndFacets",
            "variables": json.dumps({**variables, "page": 1}),
            "extensions": json.dumps(extensions)
        },
        headers=headers,
        proxies=proxies,
        timeout=20
    ).json()["data"]["productsAndFacets"]["totalCount"]

    total_pages = math.ceil(total_count / variables["pageSize"])
    logger.info(f"📦 Total products: {total_count}, Pages: {total_pages}")

    urls = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
        for fut in as_completed(futures):
            urls.extend(fut.result())

    return urls


# ----------------- Step 2: Full product parser -----------------
def clean_and_save_product_data_only_available_with_all_images_from_data(
    html_data, gender_tag=None
):
    # Parse HTML to find the __NEXT_DATA__ script
    soup = BeautifulSoup(html_data, 'html.parser')
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if not script_tag:
        logger.warning("No __NEXT_DATA__ script tag found in HTML")
        return []

    # Parse JSON data
    try:
        data = json.loads(script_tag.string)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON: {e}")
        return []

    cleaned_products = {}

    # Helper function to safely extract price
    def extract_price(price_data, source, product_id, main_product_price=None):
        try:
            if isinstance(price_data, (int, float)):
                return float(price_data)
            if isinstance(price_data, str):
                return float(price_data.strip('$').replace(',', ''))
            if isinstance(price_data, dict):
                for key in ('value', 'amount', 'price', 'currentPrice', 'sellPrice'):
                    if key in price_data:
                        return float(price_data[key])
                prices = price_data.get('prices', [])
                if prices and isinstance(prices[0], dict):
                    for key in ('value', 'amount', 'price'):
                        if key in prices[0]:
                            return float(prices[0][key])
                if main_product_price is not None and source.startswith('variant'):
                    return main_product_price
            return 0.0
        except Exception:
            return 0.0

    # Get apolloClientCache
    cache = data.get('props', {}).get('pageProps', {}).get('apolloClientCache', {})

    # Find all products with keys like Product:{"productId":"*"}
    for key, product_data in cache.items():
        if key.startswith('Product:{"productId":"'):
            handle = product_data.get('productId', 'unknown')
            title = product_data.get('displayName', 'Unknown Title')
            html_description = product_data.get('htmlDescription', '')
            vendor = product_data.get('brands', ['River Island'])[0]
            category_val = gender_tag.lower() if gender_tag else 'women'

            # Extract product type from breadcrumbs
            type_val = 'Apparel'
            breadcrumbs = product_data.get('breadcrumbs', [])
            for breadcrumb in breadcrumbs:
                if 'f-cat=' in breadcrumb.get('href', ''):
                    type_val = breadcrumb.get('title', 'Apparel')
                    break

            # Extract tags
            product_tags = ['RI Studio Collection']
            gender_tags = set()
            if gender_tag:
                if gender_tag.lower() == 'men':
                    gender_tags = {'all clothing men', 'mens', 'men clothing', 'men'}
                elif gender_tag.lower() == 'women':
                    gender_tags = {'all clothing women', 'womens', 'women clothing', 'women'}
            all_tags = product_tags + list(gender_tags) + [type_val]
            product_tags = ', '.join(tag.strip() for tag in all_tags if tag.strip())

            # Extract price (USD)
            main_price = extract_price(product_data.get('priceInfo', {}), 'main product', handle)

            # Extract sizes and variants
            variants = product_data.get('variants', [])
            in_stock = product_data.get('isInStock', False)

            # Extract images
            images = [img.get('url') for img in product_data.get('images', []) if img.get('url')]

            if in_stock:
                cleaned_products[handle] = {
                    'Handle': handle,
                    'Title': title,
                    'Body (HTML)': html_description,
                    'Vendor': vendor,
                    'Product Category': category_val,
                    'Type': type_val,
                    'Tags': product_tags,
                    'variants': []
                }

                seen = set()
                for variant in variants:
                    size = variant.get('dimensions', [{}])[0].get('value', '')
                    sku = f"{handle}-{size}"
                    if (size, sku) not in seen and variant.get('inventoryQuantity', 0) > 0:
                        variant_price = extract_price(
                            variant.get('priceInfo', {}),
                            f"variant {sku}",
                            handle,
                            main_product_price=main_price
                        )
                        cleaned_products[handle]['variants'].append({
                            'Variant SKU': sku,
                            'size': size,
                            'color': product_data.get('colour', ''),
                            'Variant Price': variant_price,
                            'Variant Compare At Price': 0.0,
                            'images': images
                        })
                        seen.add((size, sku))

    return list(cleaned_products.values())


def fetch_product_detail(url, gender_tag="women"):
    try:
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=40)
        resp.raise_for_status()
        products = clean_and_save_product_data_only_available_with_all_images_from_data(
            resp.text, gender_tag=gender_tag
        )
        print(f"✅ Done extracting product: {url}")
        return products
    except Exception as e:
        logger.warning(f"❌ Error fetching product {url}: {e}")
        print(f"❌ Failed to extract product: {url}")
        return []



def complete_workflow_river():
    all_urls = get_all_product_urls()
    logger.info(f"Collected {len(all_urls)} product URLs")
    print(f"🔍 Starting to process {len(all_urls)} products...")

    all_products = []
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_product_detail, url): url for url in all_urls}
        for fut in as_completed(futures):
            products = fut.result()
            all_products.extend(products)
            processed_count += 1
            print(f"📊 Progress: {processed_count}/{len(all_urls)} products processed ({len(products)} variants extracted)")

    # Save final JSON
    upsert_all_product_data(all_products,BASE_URL,"USD")
    with open("all_products.json", "w", encoding="utf-8") as f:
        json.dump(all_products, f, indent=2, ensure_ascii=False)


    logger.info(f"🎉 Done! Saved {len(all_products)} products to all_products.json")
    print(f"🎉 COMPLETE! Total products saved: {len(all_products)}")

# ----------------- Step 3: Orchestration -----------------
if __name__ == "__main__":
    complete_workflow_river()
