import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
from db import upsert_product

# Constants
BASE_URL = "https://www.cruisefashion.com"
GRAPHQL_URL = "https://api.prd-brands.services.frasers.io/graphql?op=getProducts"

# Default headers for web requests
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
}

# GraphQL headers
GRAPHQL_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "origin": "https://www.cruisefashion.com",
    "x-graphql-client-name": "web-crus",
    "x-graphql-client-version": "ac2c898",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, zstd",
}

# GraphQL query template
GRAPHQL_QUERY = """query getProducts($locale: Locale!, $currency: Currency!, $storeKey: String!, $colourCodes: [String!]) {
  products(colourCodes: $colourCodes, locale: $locale, currency: $currency, storeKey: $storeKey) {
    ...product
  }
}
fragment product on Product {
  styleCode
  id
  key
  name
  description
  attributes {
    ...productAttributes
  }
  featuredAttributes {
    ...featuredAttribute
  }
  variants {
    ...variant
  }
  firstVariant {
    ...displayVariant
  }
  styleSequence
}
fragment variant on CustomProductVariant {
  attributes {
    ...variantAttributes
  }
  images {
    url
    label
    type
  }
  isOnStock
  preorderAvailableDate
  size
  sku
  sizeSequence
  stockLevel
  stockLevelThreshold {
    ...stockLevelThreshold
  }
  price {
    ...customPrice
  }
  ticketPrice {
    ...customPrice
  }
  frasersPlusPrice {
    ...frasersPlusPrice
  }
}
fragment stockLevelThreshold on StockLevelThreshold {
  low
  medium
}
fragment variantAttributes on VariantAttributes {
  productType
  sashName
  sashURL
  maxPurchase
  smallImageSashUrl
  largeImageSashUrl
  textSashes {
    ...textSash
  }
  pegiRating
}
fragment textSash on TextSash {
  backgroundColour
  displayText
  position
  textColour
}
fragment customPrice on CustomPrice {
  value {
    ...customPriceValue
  }
}
fragment customPriceValue on CustomPriceValue {
  centAmount
  currency
}
fragment frasersPlusPrice on FrasersPlusPrice {
  price {
    ...customPrice
  }
  ticketPrice {
    ...customPrice
  }
  ticketPriceLabel
}
fragment displayVariant on CustomProductVariant {
  attributes {
    productType
    largeImageSashUrl
    sashName
    sashURL
    smallImageSashUrl
    textSashes {
      ...textSash
    }
  }
  sku
  preorderAvailableDate
  mainImage {
    url
  }
  price {
    ...customPrice
  }
  ticketPrice {
    ...customPrice
  }
  frasersPlusPrice {
    ...frasersPlusPrice
  }
}
fragment productAttributes on ProductAttributes {
  brand
  activity
  activityGroup
  category
  subCategory
  department
  categoryCode
  color
  gender
  url
  isDropshipProduct
  isOversized
  relatedCategories {
    name
    url
  }
  textSashes {
    ...textSash
  }
}
fragment featuredAttribute on FeaturedAttribute {
  name
  value
}"""


# ===== WEB SCRAPING MODULE =====
def get_last_page(base_url=BASE_URL, brand="ami-paris", category="Clothing", headers=DEFAULT_HEADERS):
    """Get the last page number from pagination"""
    url = f"{base_url}/{brand}?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&category.en-GB={category}&dcp=1"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    pagination_links = soup.select('[data-testid="pagination-item"]')
    page_numbers = [int(link.get_text()) for link in pagination_links if link.get_text().isdigit()]
    return max(page_numbers, default=1)


def extract_color_codes_from_page(url, headers=DEFAULT_HEADERS):
    """Extract color codes from a single page"""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    color_codes = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "#colcode=" in href:
            color_code = href.split("#colcode=")[-1]
            color_codes.append(color_code)

    return color_codes


def scrape_all_pages(base_url=BASE_URL, brand="ami-paris", category="Clothing", headers=DEFAULT_HEADERS, delay=1):
    """Scrape all pages to collect color codes"""
    last_page = get_last_page(base_url, brand, category, headers)
    print(f"[i] Total pages: {last_page}")
    all_codes = []

    for page in range(1, last_page + 1):
        print(f"[→] Scraping page {page}")
        url = f"{base_url}/{brand}?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&category.en-GB={category}&dcp={page}"
        codes = extract_color_codes_from_page(url, headers)
        all_codes.extend(codes)
        time.sleep(delay)  # Be polite

    return all_codes


def save_color_codes_to_csv(color_codes, filename="color_codes.csv"):
    """Save color codes to CSV file"""
    df = pd.DataFrame(color_codes, columns=["Color Code"])
    df.to_csv(filename, index=False)
    print(f"[✓] Done. Extracted {len(color_codes)} color codes to {filename}")
    return df


# ===== GRAPHQL API MODULE =====
def fetch_product_data(color_codes, currency="GBP", locale="en-GB", store_key="CRUS", headers=GRAPHQL_HEADERS, batch_size=8):
    """Fetch product data from GraphQL API using color codes, processing in batches of 8 at a time"""
    all_data = {"data": {"products": []}}
    
    # Process color codes in batches
    for i in range(0, len(color_codes), batch_size):
        batch = color_codes[i:i+batch_size]
        print(f"[→] Processing batch {i//batch_size + 1}/{(len(color_codes) + batch_size - 1)//batch_size} ({len(batch)} color codes)")
        
        payload = {
            "query": GRAPHQL_QUERY,
            "variables": {
                "colourCodes": batch,
                "currency": currency,
                "locale": locale,
                "storeKey": store_key
            }
        }

        response = requests.post(GRAPHQL_URL, headers=headers, json=payload)
        
        if response.ok:
            data = response.json()
            if "data" in data and "products" in data["data"]:
                all_data["data"]["products"].extend(data["data"]["products"])
                print(f"✅ Batch {i//batch_size + 1} successful: {len(data['data']['products'])} products fetched")
            else:
                print(f"⚠️ Batch {i//batch_size + 1} returned no products or unexpected format")
            
            # Be polite and avoid rate limiting
            if i + batch_size < len(color_codes):
                time.sleep(1)
        else:
            print(f"❌ Batch {i//batch_size + 1} failed: {response.status_code}")
            print(response.text)
    
    print(f"✅ Total products fetched: {len(all_data['data']['products'])}")
    return all_data


def save_json_response(data, filename="response_data.json"):
    """Save API response to JSON file"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"💾 Data saved to {filename}")






def clean_and_save_product_data(raw_json_file="response_data.json", cleaned_json_file="cleaned_products.json"):
    """Clean and deduplicate product data, then save to a new JSON file."""
    with open(raw_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    products = data.get("data", {}).get("products", [])
    cleaned_products = {}

    for product in products:
        handle = product["attributes"]["url"].split("#")[0]
        title = product["name"]
        description = f"<p>{product['description']}</p>".replace("\r\n", "<br>")
        brand = product["attributes"].get("brand", "")
        category = product["attributes"].get("category", "")
        type = product["attributes"].get("subCategory", "")
        tags = f"{product['attributes'].get('gender', '')}, {product['attributes'].get('activityGroup', '')}, {product['attributes'].get('category', '')}, {product['attributes'].get('brand', '')}"
        color = product["attributes"].get("color", "")

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category,
                "Type": type,
                "Tags": tags,
                "variants": []
            }

        # Deduplicate variants by (size, sku)
        seen = set((v["size"], v["sku"]) for v in cleaned_products[handle]["variants"])
        for variant in product.get("variants", []):
            sku = variant.get("sku", "")
            size = variant.get("size", "")
            price = variant.get("price", {}).get("value", {}).get("centAmount", 0) / 100
            compare_price = variant.get("ticketPrice", {}).get("value", {}).get("centAmount", 0) / 100
            images = [img["url"] for img in variant.get("images", [])]
            if (size, sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": price,
                    "Variant Compare At Price": compare_price,
                    "images": images
                })
                seen.add((size, sku))

    # Save cleaned data
    with open(cleaned_json_file, "w", encoding="utf-8") as f:
        json.dump({"products": list(cleaned_products.values())}, f, indent=2, ensure_ascii=False)
    print(f"[✓] Cleaned product data saved to {cleaned_json_file}")
    return {"products": list(cleaned_products.values())}


# ===== MAIN WORKFLOW FUNCTIONS =====
def scrape_and_save_color_codes(base_url=BASE_URL, brand="ami-paris", category="Clothing", output_file="color_codes.csv"):
    """Complete workflow to scrape and save color codes"""
    color_codes = scrape_all_pages(base_url, brand, category)
    save_color_codes_to_csv(color_codes, output_file)
    return color_codes


def fetch_and_save_product_data(color_codes, output_file="response_data.json"):
    """Complete workflow to fetch and save product data"""
    data = fetch_product_data(color_codes)
    if data:
        save_json_response(data, output_file)
    return data




def complete_workflow(base_url=BASE_URL, brand="ami-paris", category="Clothing", output_file="shopify_products.csv"):
    """Run the complete workflow: scraping, fetching, and processing data"""
    # Step 1: Scrape color codes
    color_codes = scrape_and_save_color_codes(base_url, brand, category)
    color_codes = color_codes[:8]
    # Step 2: Fetch product data
    data = fetch_and_save_product_data(color_codes)
    
    # Step 3: Clean and save product dat
    if data:
        cleaned_products = clean_and_save_product_data()
        upsert_product(cleaned_products, base_url,"pound")
        print(f"[✓] Complete workflow finished successfully! Final output saved to {output_file}")
    else:
        print("[❌] Workflow failed at the data fetching step.")


# Run the script if executed directly
if __name__ == "__main__":
    # Example usage
    complete_workflow()
