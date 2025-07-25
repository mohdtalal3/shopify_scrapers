import requests
import time
import json
import re
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import matplotlib.colors as mcolors

#https://notorious-plug.com/collections/amiri
#https://${p}/api/unstable/graphql.json`

        # <script id="shopify-features" type="application/json">
        #     {
        #         "accessToken": "3260355354f75aae395e213ca40bf675",
        #         "betas": [
        #             "rich-media-storefront-analytics"
        #         ],

BASE_URL = "https://notorious-plug.com"

def format_shopify_gids(product_ids):
    return [f"gid://shopify/Product/{pid}" for pid in product_ids]
def extract_product_type(title):
    # Remove "The " prefix if present

    # Split title into words
    words = title.split()
    if len(words) < 2:
        return None  # Not enough words to infer type

    # Normalize colors for comparison
    known_colors = set(name.lower() for name in mcolors.CSS4_COLORS.keys())

    # Check if second last word is a color
    second_last = words[-2].lower()
    third_last = words[-3].lower() if len(words) >= 3 else ""

    if second_last in known_colors:
        return third_last
    else:
        return second_last


def get_all_product_ids(base_url):
    def extract_last_page_number(html):
        soup = BeautifulSoup(html, "html.parser")
        pagination = soup.select(".Pagination__NavItem")
        page_numbers = [int(a.text) for a in pagination if a.text.isdigit()]
        return max(page_numbers) if page_numbers else 1

    def extract_product_ids_from_meta(html):
        pattern = r'var\s+meta\s*=\s*({.*?});\s*\n'
        match = re.search(pattern, html, re.DOTALL)
        if not match:
            return []

        meta_block = match.group(1)
        cleaned = re.sub(r',\s*([\]}])', r'\1', meta_block)

        try:
            meta_json = json.loads(cleaned)
            products = meta_json.get("products", [])
            return [p["id"] for p in products if isinstance(p, dict) and "id" in p]
        except Exception as e:
            print(f"[!] JSON error: {e}")
            return []

    all_ids = set()

    # Fetch page 1
    print(f"→ Scraping page 1: {base_url}")
    response = requests.get(base_url)
    response.raise_for_status()
    html = response.text

    # Extract from page 1
    all_ids.update(extract_product_ids_from_meta(html))
    last_page = extract_last_page_number(html)
    print(f"[✓] Total pages found: {last_page}")

    # Loop from page 2 to last
    for page in range(2, last_page + 1):
        paged_url = f"{base_url}?page={page}"
        print(f"→ Scraping page {page}: {paged_url}")
        try:
            res = requests.get(paged_url)
            res.raise_for_status()
            all_ids.update(extract_product_ids_from_meta(res.text))
        except Exception as e:
            print(f"[✗] Error on page {page}: {e}")
    print(all_ids)
    return list(all_ids)



def fetch_shopify_products_batched(product_ids):
    url = "https://notorious-plugged.myshopify.com/api/unstable/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://notorious-plug.com",
        "Referer": "https://notorious-plug.com",
        "User-Agent": "Mozilla/5.0",
        "x-shopify-storefront-access-token": "3260355354f75aae395e213ca40bf675"
    }

    query = """
    query test($ids: [ID!]!, $countryCode: CountryCode!, $languageCode: LanguageCode!) 
    @inContext(country: $countryCode, language: $languageCode) {
      nodes(ids: $ids) {
        ... on Product {
          id
          availableForSale
          title
          handle
          createdAt
          description
          descriptionHtml
          productType
          onlineStoreUrl
          options { id name values }
          featuredImage {
            id
            originalSrc
            transformedSrc(maxWidth: 800, maxHeight: 800, crop: CENTER)
          }
          updatedAt
          tags
          totalInventory
          vendor
          requiresSellingPlan
          compareAtPriceRange {
            maxVariantPrice { amount currencyCode }
            minVariantPrice { amount currencyCode }
          }
          priceRange {
            maxVariantPrice { amount currencyCode }
            minVariantPrice { amount currencyCode }
          }
          media(first: 100) {
            edges {
              node {
                id
                alt
                previewImage { url id }
              }
            }
          }
          images(first: 100) {
            edges {
              node {
                id
                originalSrc
                transformedSrc(maxWidth: 800, maxHeight: 800, crop: CENTER)
              }
            }
          }
          variants(first: 100) {
            edges {
              node {
                id
                sku
                title
                price { amount currencyCode }
                weight
                weightUnit
                requiresShipping
                currentlyNotInStock
                compareAtPrice { amount currencyCode }
                quantityAvailable
                selectedOptions { name value }
                availableForSale
                image {
                  id
                  originalSrc
                  transformedSrc(maxWidth: 800, maxHeight: 800, crop: CENTER)
                }
              }
            }
          }
        }
      }
    }
    """

    all_responses = {"data": {"nodes": []}}

    for i in range(0, len(product_ids), 100):
        batch = product_ids[i:i+100]
        payload = {
            "query": query,
            "variables": {
                "ids": batch,
                "countryCode": "US",
                "languageCode": "EN"
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                all_responses["data"]["nodes"].extend(data.get("data", {}).get("nodes", []))
                print(f"[✓] Batch {i//100+1} fetched")
            else:
                print(f"[✗] Failed batch {i//100+1}: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f"[!] Exception in batch {i//100+1}: {e}")
        time.sleep(1.2)

    # # Save to JSON file
    # with open("shopify_products.json", "w", encoding="utf-8") as f:
    #     json.dump(all_responses, f, ensure_ascii=False, indent=4)

    return all_responses


def ngrams_from_words(words, n):
    return [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]

def build_title_ngrams(title):
    words = title.strip().split()
    last3 = words[-3:] if len(words) >= 3 else words
    ngram_tags = set()
    for n in range(1, min(3, len(last3))+1):
        ngram_tags.update(ngrams_from_words(last3, n))
    return ngram_tags

def clean_and_save_product_data_only_available_with_all_images_from_data(
    data, gender_tag=None, product_type=None
):
    products = data.get("data", {}).get("nodes", [])
    cleaned_products = {}

    for product in products:
        if product is None:
            continue

        if not product.get("availableForSale", True):
            continue
        handle = product.get("handle")
        title = product.get("title")
        description = product.get("descriptionHtml") or f"<p>{product.get('description', '')}</p>"
        brand = product.get("vendor", "")
        product_tags = set(product.get("tags", []))

        # Gender-based tags
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}
            else:
                gender_tags = {"men", "women", "unisex","shoes", "unisex"}


        title_formatted = re.sub(r'\b\d+\s*Pack\b', '', title, flags=re.IGNORECASE).strip()
        title_formatted = title_formatted.replace("/", "").replace("\\", "")
        if gender_tag == "unisex":
            type_val = "shoes"
        else:
            type_val = extract_product_type(title)
        ngram_tags = build_title_ngrams(title_formatted)

        all_tags = product_tags | gender_tags | ngram_tags
        tags_str = ', '.join(sorted(all_tags))

        all_images = []
        for edge in product.get("images", {}).get("edges", []):
            url = edge["node"].get("originalSrc")
            if url:
                all_images.append(url)

        # Category is just gender
        category_val = gender_tag.lower() if gender_tag else ""


        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category_val,
                "Type": type_val,
                "Tags": tags_str,
                "variants": []
            }

        seen = set()
        for edge in product.get("variants", {}).get("edges", []):
            variant = edge["node"]
            if not variant.get("availableForSale", False):
                continue

            sku = variant.get("sku", "")
            price = float(variant.get("price", {}).get("amount", 0))
            compare_price = float(variant.get("compareAtPrice", {}).get("amount", 0)) if variant.get("compareAtPrice") else 0
            color, size = "", ""
            for opt in variant.get("selectedOptions", []):
                if opt["name"].lower() == "color":
                    color = opt["value"]
                elif opt["name"].lower() == "size":
                    size = opt["value"]

            if (size, sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": price,
                    "Variant Compare At Price": compare_price,
                    "images": list(set(all_images))
                })
                seen.add((size, sku))

    # Return as a list of product dicts
    return list(cleaned_products.values())

def complete_workflow_notorious():
    collections = [
        {"url": "https://notorious-plug.com/collections/amiri", "gender": "men"},
        {"url": "https://notorious-plug.com/collections/womens", "gender": "women"},
        {"url": "https://notorious-plug.com/collections/shoes", "gender": "unisex"},
    ]

    print("🔍 Scraping product IDs from all collections...")
    all_scraped_ids = []
    product_id_to_collection = {}  # Map product IDs to their source collection
    
    # Scrape IDs from each collection URL
    for i, collection in enumerate(collections):
        print(f"→ Processing collection {i+1}/{len(collections)}: {collection['url']}")
        try:
            collection_ids = get_all_product_ids(collection["url"])
            all_scraped_ids.extend(collection_ids)
            
            # Map each product ID to its source collection
            for product_id in collection_ids:
                product_id_to_collection[str(product_id)] = collection
            
            print(f"✓ Found {len(collection_ids)} product IDs from {collection['url']}")
        except Exception as e:
            print(f"✗ Error scraping {collection['url']}: {e}")
            continue
    
    unique_ids = list(set(all_scraped_ids))
    print(f"🎯 Total Unique Product IDs across all collections: {len(unique_ids)}")

    if not unique_ids:
        print("❌ No product IDs found. Exiting.")
        return

    gids = format_shopify_gids(unique_ids)

    print("📦 Fetching product data in batches...")
    raw_data = fetch_shopify_products_batched(gids)

    # Process products with their correct collection data
    all_products = []
    products_by_collection = {}
    
    # Group products by their source collection
    for product in raw_data.get("data", {}).get("nodes", []):
        if product is None:
            continue
        
        # Extract product ID from the Shopify GID
        product_gid = product.get("id", "")
        product_id = product_gid.split("/")[-1] if "/" in product_gid else product_gid
        
        # Find which collection this product belongs to
        source_collection = product_id_to_collection.get(str(product_id))
        if source_collection:
            collection_key = source_collection["url"]
            if collection_key not in products_by_collection:
                products_by_collection[collection_key] = {
                    "products": [],
                    "collection_info": source_collection
                }
            products_by_collection[collection_key]["products"].append(product)
    
    # Now process each collection's products with the correct gender/type
    for collection_url, collection_data in products_by_collection.items():
        products = collection_data["products"]
        collection_info = collection_data["collection_info"]
        
        gender_tag = collection_info.get("gender")
        product_type = collection_info.get("product_type")
        
        print(f"🧹 Cleaning {len(products)} products for {collection_url} (gender: {gender_tag})...")
        
        # Create a temporary data structure for this collection
        temp_data = {"data": {"nodes": products}}
        cleaned = clean_and_save_product_data_only_available_with_all_images_from_data(
            temp_data, gender_tag, product_type
        )
        all_products.extend(cleaned)

    # Remove duplicate products by handle (keep first occurrence)
    seen_handles = set()
    unique_products = []
    for prod in all_products:
        if prod["Handle"] not in seen_handles:
            unique_products.append(prod)
            seen_handles.add(prod["Handle"])

    # # Write one JSON file
    # with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
    #     json.dump({"products": unique_products}, f, ensure_ascii=False, indent=4)
    # Upload all at once
    upsert_all_product_data(unique_products, BASE_URL, "USD")
    print(f"✅ Cleaned data saved to database and written to cleaned_products_new.json.")
    print(f"📊 Total unique products processed: {len(unique_products)}")
    
    # # Show breakdown by gender
    # gender_breakdown = {}
    # for prod in unique_products:
    #     category = prod.get("Product Category", "unknown")
    #     gender_breakdown[category] = gender_breakdown.get(category, 0) + 1
    
    # print("📈 Products by gender:")
    # for gender, count in gender_breakdown.items():
    #     print(f"   {gender}: {count} products")


# 🔧 Run Everything
if __name__ == "__main__":

    complete_workflow_notorious()


