"""
The Reformation Product Scraper

This scraper fetches product data from The Reformation using a two-step process:
1. Fetch category pages to collect product IDs
2. Fetch detailed product data for each ID

RETRY MECHANISMS:
- All HTTP requests have configurable retry logic with exponential backoff
- Page fetching retries if zero products are found
- Product detail requests retry on failure or zero products
- Failed pages/products are retried once more with single threading
- Configurable retry parameters: DEFAULT_RETRIES, DEFAULT_BACKOFF_FACTOR, timeouts

CONFIGURATION:
- DEFAULT_RETRIES: Number of retry attempts (default: 3)
- DEFAULT_BACKOFF_FACTOR: Exponential backoff multiplier (default: 2)
- DEFAULT_TIMEOUT: Request timeout for page scraping (default: 100s)
- DEFAULT_PRODUCT_TIMEOUT: Request timeout for product details (default: 100s)
"""

from curl_cffi import requests
import re
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

# Load environment variables
load_dotenv()
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None
print(proxies)
BASE_URL = "https://www.thereformation.com"

# Retry Configuration
DEFAULT_RETRIES = 4
DEFAULT_BACKOFF_FACTOR = 2
DEFAULT_TIMEOUT = 100
DEFAULT_PRODUCT_TIMEOUT = 100

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Referer': 'https://www.thereformation.com/',
}
def clean_handle(name):
    """Convert product name to a clean handle format."""
    return re.sub(r'\s+', '-', name.lower().strip())

def generate_image_urls(base_url, product_id, color_id, max_images=5):
    """Generate up to max_images URLs by modifying the index in the base URL."""
    images = [base_url] if base_url else []
    if base_url:
        base_pattern = re.sub(r'\.\d+\.\w+', '.{index}.' + color_id.upper(), base_url)
        for index in range(2, max_images + 1):
            generated_url = base_pattern.format(index=index)
            images.append(generated_url)
    return images

def extract_product_ids(html_content):
    """Extract product IDs from product page URLs in the HTML content."""
    product_ids = set()
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a', href=re.compile(r'/products/[^/]+/[^/]+\.html'))
    
    for link in links:
        href = link.get('href')
        match = re.search(r'/products/[^/]+/([^/]+)\.html', href)
        if match:
            product_ids.add(match.group(1))
    
    return product_ids

def fetch_page_product_ids(base_url, start, page_size, retries=DEFAULT_RETRIES, backoff_factor=DEFAULT_BACKOFF_FACTOR):
    """Fetch a category page and extract product IDs with retry mechanism."""
    url = f"{base_url}&start={start}&sz={page_size}"
    page_num = start // page_size + 1
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, proxies=proxies, timeout=DEFAULT_TIMEOUT, impersonate="chrome131")
            response.raise_for_status()
            product_ids = extract_product_ids(response.text)
            
            # Check if page has zero products and retry if it's not the last attempt
            if len(product_ids) == 0:
                print(f"[⚠️] Zero products found on page {page_num}, attempt {attempt+1}")
                if attempt < retries - 1:
                    time.sleep(backoff_factor ** attempt)
                    continue
                else:
                    print(f"[⚠️] Zero products found on page {page_num} after {retries} attempts")
            
            return page_num, product_ids
        except Exception as e:
            print(f"[!] Error fetching page {page_num}, attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"[❌] Failed to fetch page {page_num} after {retries} attempts")
                return page_num, set()

def fetch_product_details(base_url, pid, retries=DEFAULT_RETRIES, backoff_factor=DEFAULT_BACKOFF_FACTOR):
    """Fetch product details from the Product-ShowQuickAdd endpoint with retry mechanism."""
    quick_add_url = urljoin(base_url, f"/on/demandware.store/Sites-reformation-us-Site/en_US/Product-ShowQuickAdd?pid={pid}&gtmListAttribute=Category%3A%20Shorts&pageTypeContext=Search-Show")
    
    for attempt in range(retries):
        try:
            response = requests.get(quick_add_url, headers=HEADERS, proxies=proxies, timeout=DEFAULT_PRODUCT_TIMEOUT, impersonate="chrome131")
            response.raise_for_status()
            data = response.json()
            
            # Check if response contains valid product data
            if not data or not data.get('product'):
                print(f"[⚠️] No product data for {pid}, attempt {attempt+1}")
                if attempt < retries - 1:
                    time.sleep(backoff_factor ** attempt)
                    continue
                else:
                    print(f"[⚠️] No product data for {pid} after {retries} attempts")
            
            return pid, data
        except (Exception, json.JSONDecodeError) as e:
            print(f"[!] Error fetching product details for {pid}, attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"[❌] Failed to fetch product details for {pid} after {retries} attempts")
                return pid, None

def format_product_data(product_data, base_url):
    """Format product data according to the specified structure, excluding out-of-stock variants."""
    product = product_data.get('product', {})
    
    if not product.get('purchasable', False):
        return None
    
    # Validate essential product fields
    title = product.get('productName', '')
    if not title or title.strip() == '':
        print(f"Skipping product: Title is None or empty")
        return None
    
    handle = clean_handle(title)
    vendor = product.get('brand', '') or 'Unknown'
    product_category = 'women'
    product_type = product.get('item_class', 'Unknown')
    
    material_description = product.get('material_description', '') or ''
    country_of_origin = product.get('country_of_origin_sustainabilitytext', '') or ''
    body_html = f"<p>{title}, {material_description.lower()} {country_of_origin.lower()}.</p>"
    
    tags = f"{product_type}, sustainable, women, eco-friendly, {vendor}, women's , clothing"
    
    variants = []
    variation_attributes = product.get('variationAttributes', [])
    if not variation_attributes:
        print(f"Skipping product {title}: No variation attributes found")
        return None
    
    color_variations = variation_attributes[0].get('values', []) if variation_attributes else []
    # if not color_variations:
    #     print(f"Skipping product {title}: No color variations found")
    #     return None
    
    for color_variant in color_variations:
        color = color_variant.get('displayValue', '')
        color_id = color_variant.get('id', '')
        product_id = color_variant.get('productId', '')
        
        base_image = next(
            (img.get('absURL', '') for img in color_variant.get('images', {}).get('medium', []) 
             if img.get('hasImage', True)), 
            ''
        )
        images = generate_image_urls(base_image, product_id, color_id, max_images=5)
        
        size_variants = next(
            (attr for attr in product.get('variationAttributes', []) 
             if attr.get('attributeId') == 'sizeByColor'), 
            {}
        ).get('values', [])
        
        for size_variant in size_variants:
            if size_variant.get('color', {}).get('id') == color_id:
                for size in size_variant.get('sizes', []):
                    variant_product = size.get('product', {})
                    availability = variant_product.get('availability', {})
                    if not (variant_product.get('available', False) and variant_product.get('purchasable', False)):
                        continue
                    if 'Out of Stock' in availability.get('messages', []):
                        continue
                    
                    # Validate essential variant fields
                    variant_sku = variant_product.get('id', '')
                    
                    size_value = size.get('displayValue', '')

                    price = variant_product.get('price', {}).get('sales', {}).get('value')
                    if price is None or price == 0 or price == '':
                        print(f"Skipping variant: Price is None, empty, or 0 for product {title}, variant {variant_sku}")
                        continue
                    
                    try:
                        price_float = float(price)
                        if price_float <= 0:
                            print(f"Skipping variant: Invalid price {price} for product {title}, variant {variant_sku}")
                            continue
                    except (ValueError, TypeError):
                        print(f"Skipping variant: Cannot convert price to float for product {title}, variant {variant_sku}")
                        continue
                    
                    variant = {
                        "Variant SKU": variant_sku,
                        "size": size_value,
                        "color": color,
                        "Variant Price": price_float,
                        "Variant Compare At Price": 0,
                        "images": images
                    }
                    variants.append(variant)
    
    if not variants:
        print(f"Skipping product {title}: No valid variants found")
        return None
    
    # Validate other essential product fields before returning
    if not handle or handle.strip() == '':
        print(f"Skipping product {title}: Handle is invalid")
        return None
    
    return {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": body_html,
        "Vendor": vendor,
        "Product Category": product_category,
        "Type": product_type,
        "Tags": tags,
        "variants": variants
    }

def extract_and_format_all_products(category_url, output_file, max_pages=100, max_workers=30):
    """Extract unique product IDs from all pages and format their data using threading."""
    base_url = "https://www.thereformation.com"
    all_product_ids = set()
    page_size = 24
    failed_pages = []
    
    # Step 1: Fetch category pages concurrently to collect product IDs
    print("Starting to fetch category pages...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        page_futures = [
            executor.submit(fetch_page_product_ids, category_url, page * page_size, page_size)
            for page in range(max_pages)
        ]
        for future in as_completed(page_futures):
            try:
                page_num, product_ids = future.result()
                if product_ids:
                    all_product_ids.update(product_ids)
                    print(f"[✓] Page {page_num} completed: Found {len(product_ids)} product IDs")
                else:
                    failed_pages.append(page_num)
                    print(f"[!] Page {page_num} completed: No product IDs found")
            except Exception as e:
                print(f"[!] Error processing page future: {e}")
    
    # Retry failed pages one more time with single threading
    if failed_pages:
        print(f"[i] Retrying {len(failed_pages)} failed pages with single threading...")
        for page_num in failed_pages:
            try:
                start = (page_num - 1) * page_size
                _, product_ids = fetch_page_product_ids(category_url, start, page_size)
                if product_ids:
                    all_product_ids.update(product_ids)
                    print(f"[✓] Retry successful: Page {page_num} found {len(product_ids)} product IDs")
                else:
                    print(f"[!] Retry failed: Page {page_num} still has no product IDs")
            except Exception as e:
                print(f"[!] Retry failed for page {page_num}: {e}")
    
    print(f"✅ Total unique product IDs collected: {len(all_product_ids)}")
    
    # Step 2: Fetch product details concurrently
    formatted_data = []
    failed_products = []
    print("Starting to fetch product details...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pid = {executor.submit(fetch_product_details, base_url, pid): pid for pid in all_product_ids}
        for future in as_completed(future_to_pid):
            pid = future_to_pid[future]
            try:
                _, product_data = future.result()
                if product_data:
                    formatted_product = format_product_data(product_data, base_url)
                    if formatted_product:
                        formatted_data.append(formatted_product)
                        print(f"[✓] Processed product {pid}")
                    else:
                        print(f"[!] No in-stock variants for product {pid}")
                else:
                    failed_products.append(pid)
                    print(f"[!] No data returned for product {pid}")
            except Exception as e:
                failed_products.append(pid)
                print(f"[!] Error processing product {pid}: {e}")
    
    # Retry failed products one more time with single threading
    if failed_products:
        print(f"[i] Retrying {len(failed_products)} failed products with single threading...")
        for pid in failed_products:
            try:
                _, product_data = fetch_product_details(base_url, pid)
                if product_data:
                    formatted_product = format_product_data(product_data, base_url)
                    if formatted_product:
                        formatted_data.append(formatted_product)
                        print(f"[✓] Retry successful: Processed product {pid}")
                    else:
                        print(f"[!] Retry failed: No in-stock variants for product {pid}")
                else:
                    print(f"[!] Retry failed: No data returned for product {pid}")
            except Exception as e:
                print(f"[!] Retry failed for product {pid}: {e}")

    upsert_all_product_data(formatted_data, BASE_URL, "USD")
    # Save to output JSON file
    with open(output_file, 'w') as f:
        json.dump(formatted_data, f, indent=4)
    
    print(f"Formatted data for {len(formatted_data)} products written to {output_file}")

def complete_workflow_thereformation(max_pages=100, max_workers=30, retries=DEFAULT_RETRIES):
    """Run the complete The Reformation scraping workflow with configurable parameters."""
    category_url = "https://www.thereformation.com/on/demandware.store/Sites-reformation-us-Site/en_US/Search-ShowAjax?cgid=clothing&pmpt=qualifying&prefn1=subclass&prefv1=Dresses%7cTops%7cTees%7cJeans%7cJumpsuits%7cPants%7cTwo%20pieces%7cSweatshirts%7cSweaters%7cSkirts%7cOuterwear%7cOne%2bPiece&srule=Best%20of"
    output_file = "all_products.json"
    print(f"[i] Starting The Reformation scraper with {retries} retries, {max_workers} workers, max {max_pages} pages")
    extract_and_format_all_products(category_url, output_file, max_pages, max_workers)


# Example usage
if __name__ == "__main__":
    complete_workflow_thereformation()