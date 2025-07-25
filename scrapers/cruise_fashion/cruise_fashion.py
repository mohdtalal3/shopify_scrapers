import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
from db import upsert_all_product_data
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
load_dotenv()
proxy_str = os.getenv("PROXY_URL")

# Proxies dictionary for requests
proxies = {
    "http": proxy_str,
    "https": proxy_str
}


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
def get_last_page_from_url(url, headers=DEFAULT_HEADERS):
    """Get the last page number from pagination for a given full URL (page 1)"""
    response = requests.get(url, headers=headers, proxies=proxies)
    soup = BeautifulSoup(response.text, "html.parser")
    pagination_links = soup.select('[data-testid="pagination-item"]')
    page_numbers = [int(link.get_text()) for link in pagination_links if link.get_text().isdigit()]
    return max(page_numbers, default=1)


def extract_color_codes_from_page(url, headers=DEFAULT_HEADERS):
    """Extract color codes from a single page (URL)"""
    response = requests.get(url, headers=headers, proxies=proxies)
    soup = BeautifulSoup(response.text, "html.parser")
    color_codes = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "#colcode=" in href:
            color_code = href.split("#colcode=")[-1]
            color_codes.append(color_code)

    return color_codes


def scrape_all_pages_from_urls(urls, headers=DEFAULT_HEADERS, delay=1, max_workers=5):
    """Scrape all provided URLs (and their paginated pages) to collect color codes, using threading."""
    all_codes = []
    for url in urls:
        print(f"[→] Processing base URL: {url}")
        # Remove dcp=... if present, we'll add it for pagination
        if "&dcp=" in url:
            base_url = url.split("&dcp=")[0]
        else:
            base_url = url
        # Always start with page 1
        first_page_url = base_url + ("&dcp=1" if "&dcp=" not in url else "")
        last_page = get_last_page_from_url(first_page_url, headers)
        print(f"[i] Total pages for this URL: {last_page}")
        page_urls = [f"{base_url}&dcp={page}" for page in range(1, last_page + 1)]
        codes = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(extract_color_codes_from_page, page_url, headers): page_url for page_url in page_urls}
            for future in as_completed(future_to_url):
                page_url = future_to_url[future]
                try:
                    result = future.result()
                    codes.extend(result)
                    print(f"[✓] Scraped {len(result)} codes from {page_url}")
                except Exception as exc:
                    print(f"[!] Error scraping {page_url}: {exc}")
                time.sleep(delay)
        all_codes.extend(codes)
    return all_codes


def save_color_codes_to_csv(color_codes, filename="color_codes.csv"):
    """Save color codes to CSV file"""
    df = pd.DataFrame(color_codes, columns=["Color Code"])
    df.to_csv(filename, index=False)
    print(f"[✓] Done. Extracted {len(color_codes)} color codes to {filename}")
    return df


# ===== GRAPHQL API MODULE =====
def fetch_product_data(color_codes, currency="GBP", locale="en-GB", store_key="CRUS", headers=GRAPHQL_HEADERS, batch_size=100, max_workers=5):
    """Fetch product data from GraphQL API using color codes, processing in batches of 8 at a time, with threading."""
    all_data = {"data": {"products": []}}
    batches = [color_codes[i:i+batch_size] for i in range(0, len(color_codes), batch_size)]
    print(f"[i] Total batches: {len(batches)}")

    def fetch_batch(batch, batch_num):
        print(f"[→] Processing batch {batch_num}/{len(batches)} ({len(batch)} color codes)")
        payload = {
            "query": GRAPHQL_QUERY,
            "variables": {
                "colourCodes": batch,
                "currency": currency,
                "locale": locale,
                "storeKey": store_key
            }
        }
        response = requests.post(GRAPHQL_URL, headers=headers, json=payload, proxies=proxies)
        if response.ok:
            data = response.json()
            if "data" in data and "products" in data["data"]:
                print(f"✅ Batch {batch_num} successful: {len(data['data']['products'])} products fetched")
                return data["data"]["products"]
            else:
                print(f"⚠️ Batch {batch_num} returned no products or unexpected format")
                return []
        else:
            print(f"❌ Batch {batch_num} failed: {response.status_code}")
            print(response.text)
            return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batchnum = {executor.submit(fetch_batch, batch, i+1): i+1 for i, batch in enumerate(batches)}
        for future in as_completed(future_to_batchnum):
            batch_num = future_to_batchnum[future]
            try:
                products = future.result()
                all_data["data"]["products"].extend(products)
            except Exception as exc:
                print(f"[!] Error in batch {batch_num}: {exc}")
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
        seen = set((v.get("size", ""), v.get("sku", "")) for v in cleaned_products[handle]["variants"])
        for variant in product.get("variants", []):
            sku = variant.get("sku", "")
            size = variant.get("size", "")
            price = variant.get("price", {}).get("value", {}).get("centAmount", 0) / 100
            compare_price = variant.get("ticketPrice", {}).get("value", {}).get("centAmount", 0) / 100
            images = [img.get("url", "") for img in variant.get("images", [])]
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
    return list(cleaned_products.values())


# ===== MAIN WORKFLOW FUNCTIONS =====
def scrape_and_save_color_codes_from_urls(urls, output_file="color_codes.csv"):
    """Complete workflow to scrape and save color codes from a list of URLs"""
    color_codes = scrape_all_pages_from_urls(urls)
    save_color_codes_to_csv(color_codes, output_file)
    return color_codes


def fetch_and_save_product_data(color_codes, output_file="response_data.json"):
    """Complete workflow to fetch and save product data"""
    data = fetch_product_data(color_codes)
    if data:
        save_json_response(data, output_file)
    return data




def complete_workflow_cruise_fashion():
    urls = [
      # Put your URLs here
      "https://www.cruisefashion.com/ami-paris?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&category.en-GB=Clothing",
      #"https://www.cruisefashion.com/outlet/sandals?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&webgender.en-GB=Womens%2CMens&webbrand.en-GB=Off+White&webcat.en-GB=Earrings%2CTops+and+T-Shirts%2CHoodies+and+Sweatshirts%2CTrousers%2CCoats+and+Jackets%2CDresses%2CTrainers%2CShorts%2CShirts%2CHandbags%2CJeans%2CShoes%2CHats+and+Caps",
       "https://www.cruisefashion.com/outlet/hats-and-caps?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&webgender.en-GB=Mens%2CWomens&webbrand.en-GB=Palm+Angels&webcat.en-GB=Hoodies+and+Sweatshirts%2CTops+and+T-Shirts%2CCoats+and+Jackets%2CTracksuits%2CShirts%2CTrainers%2CShorts%2CJeans%2CShoes",
      #"https://www.cruisefashion.com/outlet/golden-goose?sort=DISCOUNT_PERCENTAGE&sortDirection=DESC&webgender.en-GB=Mens%2CWomens&webbrand.en-GB=Off+White%2CHugo%2CEmporio+Armani%2CVivienne+Westwood%2CPalm+Angels%2CHeron+Preston%2CPolo+Ralph+Lauren%2CMoschino%2CValentino+Garavani%2CTom+Ford%2CAmiri%2CBurberry%2CDolce+and+Gabbana%2CJacquemus%2CBalenciaga%2CVersace%2CRepresent%2CDiesel%2CAmi+Paris%2CBoss%2CCasablanca%2CGucci%2CAxel+Arigato%2CValentino%2CBalmain%2CAmbush%2CJimmy+Choo%2CMarcelo+Burlon%2CVersace+Jeans+Couture%2CKenzo%2CLanvin%2CRepresent+247%2CNeil+Barrett%2CSaint+Laurent%2CChloe%2CPurple+Brand%2CLove+Moschino%2CRhude%2CVETEMENTS%2CMarc+Jacobs%2CVersace+Icon",
       "https://www.cruisefashion.com/outlet/represent"
  ]
    """Run the complete workflow: scraping, fetching, and processing data from a list of URLs"""
    # Step 1: Scrape color codes
    color_codes = scrape_and_save_color_codes_from_urls(urls)
    #color_codes = color_codes[:8]
    # Step 2: Fetch product data
    data = fetch_and_save_product_data(color_codes)
    # Step 3: Clean and save product data
    if data:
        cleaned_products = clean_and_save_product_data()
        upsert_all_product_data(cleaned_products, BASE_URL, "GBP")
    else:
        print("[❌] Workflow failed at the data fetching step.")


# Run the script if executed directly
if __name__ == "__main__":
    # Example usage: provide a list of URLs

    complete_workflow_cruise_fashion()
