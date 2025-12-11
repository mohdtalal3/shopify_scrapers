import requests
import re
import json
from dotenv import load_dotenv
import os
import sys
import time
from seleniumbase import SB
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

# Load environment variables
load_dotenv()
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None
BASE_URL="https://laperla.com"

def extract_authorization_token(json_file_path):
    """Extract Authorization token from CDP performance logs"""
    authorization_token = None

    with open(json_file_path, "r", encoding="utf-8") as f:
        cdp_logs = json.load(f)

    for entry in cdp_logs:
        msg_str = entry.get("message")
        if not msg_str:
            continue

        try:
            parsed = json.loads(msg_str)
        except:
            continue

        if not parsed or "message" not in parsed:
            continue

        message = parsed["message"]
        params = message.get("params", {})

        # Check request headers
        if "request" in params and isinstance(params["request"], dict):
            headers = params["request"].get("headers", {})
            for key, value in headers.items():
                if key.lower() == "authorization":
                    authorization_token = value
                    break

        # Check extra request headers
        if message.get("method") == "Network.requestWillBeSentExtraInfo":
            headers = params.get("headers", {})
            for key, value in headers.items():
                if key.lower() == "authorization":
                    authorization_token = value
                    break

        # Check response headers
        if "response" in params and isinstance(params["response"], dict):
            headers = params["response"].get("headers", {})
            for key, value in headers.items():
                if key.lower() == "authorization":
                    authorization_token = value
                    break

        if authorization_token:
            break

    return authorization_token


def get_authorization_from_browser(url="https://us.laperla.com/?setCurrencyId=2"):
    """Open browser and extract authorization token"""
    try:
        sb_kwargs = {"uc": True, "headless": True, "log_cdp_events": True}
        with SB(**sb_kwargs) as sb:
            sb.open(url)
            sb.wait_for_element("select#gle_selectedCountry", timeout=300)
            sb.select_option_by_value("select#gle_selectedCountry", "US")
            time.sleep(4)
            sb.click("#saveNcloseBtn")
            time.sleep(4)
            sb.open("https://us.laperla.com/lingerie/")
            time.sleep(4)
            
            # Get CDP logs
            cdp_logs = sb.driver.get_log("performance")
            logs_file_path = os.path.abspath("cdp_logs.json")
            with open(logs_file_path, "w", encoding="utf-8") as f:
                json.dump(cdp_logs, f, indent=2, ensure_ascii=False)

            # Extract token
            token = extract_authorization_token(logs_file_path)
            
            # Clean up log file
            if os.path.exists(logs_file_path):
                os.remove(logs_file_path)
            
            return token
            
    except Exception as e:
        print(f"Failed to extract authorization token: {str(e)}")
        return None

# Define comprehensive headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Origin': 'https://us.laperla.com',
    'Referer': 'https://us.laperla.com/lingerie/?setCurrencyId=2',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin'
}

# GraphQL query (provided)
GRAPHQL_QUERY = """
query productsInCategory(
    $pageSize: Int = 50
    $cursor: String = ""
) {
    site {
        search {
            searchProducts(filters: {
                categoryEntityId: 29
            }, sort: FEATURED) {
                products(first: $pageSize, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        node {
                            name
                            sku
                            entityId
                            path
                            brand {
                                name
                            }
                            prices {
                                salePrice {
                                    formatted
                                    value
                                }
                                basePrice {
                                    formatted
                                    value
                                }
                            }
                            customFields {
                                edges {
                                    node {
                                        entityId
                                        name
                                        value
                                    }
                                }
                            }
                            images(first: 2) {
                                edges {
                                    node {
                                        altText
                                        url(width: 640)
                                    }
                                }
                            }
                            variants {
                                edges {
                                    node {
                                        inventory {
                                            isInStock
                                        }
                                        options {
                                            edges {
                                                node {
                                                    displayName
                                                    entityId
                                                    values {
                                                        edges {
                                                            node {
                                                                entityId
                                                                label
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

def clean_handle(name):
    """Convert product name to a clean handle format."""
    return re.sub(r'\s+', '-', name.lower().strip())

def fetch_page_products(graphql_url, cursor=None, page_size=50):
    """Fetch a page of products using the GraphQL endpoint."""
    variables = {"pageSize": page_size}
    if cursor:
        variables["cursor"] = cursor
    
    payload = {"query": GRAPHQL_QUERY, "variables": variables}
    
    try:
        response = requests.post(graphql_url, headers=HEADERS, json=payload,  proxies=proxies, timeout=100)
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('site', {}).get('search', {}).get('searchProducts', {}).get('products', {})
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Error fetching page with cursor {cursor or 'initial'}: {e}")
        return None

def format_product_data(product):
    """Format product data according to the specified structure, excluding out-of-stock variants."""
    name = product.get('name', '')
    handle = clean_handle(name)
    vendor = product.get('brand', {}).get('name', '')
    product_category = 'women'
    product_type = 'Lingerie'
    
    # Extract material, country, and product details from customFields
    material = next((field['node']['value'] for field in product.get('customFields', {}).get('edges', []) if field['node']['name'] == 'Material'), '')
    country = next((field['node']['value'] for field in product.get('customFields', {}).get('edges', []) if field['node']['name'] == 'country'), '')
    product_details = next((field['node']['value'] for field in product.get('customFields', {}).get('edges', []) if field['node']['name'] == 'Product Details'), '')
    body_html = f"<p>{name}, {material.lower()}. Made in {country.lower()}.</p>{product_details}"
    
    tags = f"{product_type}, sustainable, women, eco-friendly, {vendor}, women clothing , all women clothing , womens , women's"
    
    # Get price (prefer salePrice if available, else basePrice)
    prices = product.get('prices', {})
    sale_price = prices.get('salePrice') if prices else None
    base_price = prices.get('basePrice') if prices else None
    
    price = 0.0
    if sale_price and isinstance(sale_price, dict):
        price = sale_price.get('value', 0.0)
    elif base_price and isinstance(base_price, dict):
        price = base_price.get('value', 0.0)
    
    # Get all image URLs from images.edges
    images = [img['node']['url'] for img in product.get('images', {}).get('edges', [])]
    
    # Process variants
    variants = []
    for variant_edge in product.get('variants', {}).get('edges', []):
        variant = variant_edge.get('node', {})
        if not variant.get('inventory', {}).get('isInStock', False):
            continue
        
        size = None
        color = None
        for option_edge in variant.get('options', {}).get('edges', []):
            option = option_edge.get('node', {})
            if option.get('displayName') == 'Size':
                size = option.get('values', {}).get('edges', [{}])[0].get('node', {}).get('label', '')
            elif option.get('displayName') == 'Colour':
                color = option.get('values', {}).get('edges', [{}])[0].get('node', {}).get('label', '')
        
        variant = {
            "Variant SKU": product.get('sku', ''),
            "size": size,
            "color": color,
            "Variant Price": float(price),
            "Variant Compare At Price": 0,
            "images": images
        }
        variants.append(variant)
    
    if not variants:
        return None
    
    return {
        "Handle": handle,
        "Title": name,
        "Body (HTML)": body_html,
        "Vendor": vendor,
        "Product Category": product_category,
        "Type": product_type,
        "Tags": tags,
        "variants": variants
    }

def extract_and_format_all_products(graphql_url, output_file):
    """Extract all product IDs sequentially and stop when no next page exists."""
    all_product_ids = set()
    formatted_data = []
    page_size = 50
    page_num = 1
    cursor = None

    print("Starting sequential fetch of product pages...")

    while True:
        # Fetch a page
        data = fetch_page_products(graphql_url, cursor, page_size)
        if not data:
            print(f"Page {page_num} failed to fetch")
            break

        # Collect product IDs
        for edge in data.get('edges', []):
            product = edge.get('node', {})
            all_product_ids.add(str(product.get('entityId')))

        print(f"Page {page_num} completed: Found {len(data.get('edges', []))} product IDs")

        # Process product details
        for edge in data.get('edges', []):
            product = edge.get('node', {})
            formatted_product = format_product_data(product)
            if formatted_product:
                formatted_data.append(formatted_product)
                print(f"Processed product {product.get('entityId')}")
            else:
                print(f"No in-stock variants for product {product.get('entityId')}")

        # Check if there’s another page
        has_next_page = data.get('pageInfo', {}).get('hasNextPage', False)
        cursor = data.get('pageInfo', {}).get('endCursor')

        if not has_next_page or not cursor:
            print("No more pages to fetch.")
            break

        page_num += 1

    print(f"Total pages fetched: {page_num}")
    print(f"Total unique product IDs collected: {len(all_product_ids)}")
    upsert_all_product_data(formatted_data,BASE_URL,"USD")
    # Save to output JSON file
    with open(output_file, 'w') as f:
        json.dump(formatted_data, f, indent=4)

    print(f"Formatted data for {len(formatted_data)} products written to {output_file}")


def complete_workflow_laperla():
    # Extract authorization token
    print("Extracting authorization token...")
    token = get_authorization_from_browser()
    
    if not token:
        print("Failed to extract authorization token. Exiting.")
        return
    
    print(f"✓ Authorization token extracted: {token[:50]}...")
    
    # Add token to headers
    HEADERS['Authorization'] = token
    
    graphql_url = "https://us.laperla.com/graphql"
    output_file = "formatted_products.json"
    extract_and_format_all_products(graphql_url, output_file)


# Example usage
if __name__ == "__main__":
    complete_workflow_laperla()