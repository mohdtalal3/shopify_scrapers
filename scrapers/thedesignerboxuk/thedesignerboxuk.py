import requests
import time
import json
import re
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_product



BASE_URL = "https://thedesignerboxuk.com"

def format_shopify_gids(product_ids):
    return [f"gid://shopify/Product/{pid}" for pid in product_ids]


def extract_last_page(html):
    soup = BeautifulSoup(html, "html.parser")
    pagination = soup.select("div.pagination .page a")
    if not pagination:
        return 1
    try:
        return max(int(a.text) for a in pagination if a.text.isdigit())
    except:
        return 1

def extract_main_product_ids_only(html):
    """
    Extract only main product IDs from:
    window.ORDERSIFY_BIS.collection_products = window.ORDERSIFY_BIS.collection_products || [ ... ];
    """
    pattern = r'window\.ORDERSIFY_BIS\.collection_products\s*=\s*window\.ORDERSIFY_BIS\.collection_products\s*\|\|\s*(\[\s*\{.*?\}\s*\]);'
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        return set()

    json_text = match.group(1)
    try:
        products = json.loads(json_text)
        return set(product['id'] for product in products if isinstance(product, dict) and 'id' in product)
    except Exception as e:
        print("JSON parse error:", e)
        return set()

def scrape_main_product_ids(urls):
    all_main_ids = set()
    urls = [item["url"] for item in urls]
    for base_url in urls:
        try:
            print(f"\n🔍 Processing: {base_url}")
            response = requests.get(base_url)
            response.raise_for_status()
            last_page = extract_last_page(response.text)

            for page in range(1, last_page + 1):
                paged_url = f"{base_url}?page={page}"
                print(f"  📄 Fetching page {page}...")
                resp = requests.get(paged_url)
                if resp.status_code == 200:
                    html = resp.text
                    main_ids = extract_main_product_ids_only(html)
                    all_main_ids.update(main_ids)

        except Exception as e:
            print(f"❌ Error processing {base_url}: {e}")

    return sorted(all_main_ids)




def fetch_shopify_products_batched(product_ids):
    url = "https://thedesignerbox.myshopify.com/api/2024-01/graphql.json"

    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://thedesignerboxuk.com",
        "Referer": "https://thedesignerboxuk.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-shopify-storefront-access-token": "d2e6ee62da9d5158adadada8c59c4bb1"
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

    # # # Save to JSON file
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


        all_tags = product_tags | gender_tags 
        tags_str = ', '.join(sorted(all_tags))

        all_images = []
        for edge in product.get("images", {}).get("edges", []):
            url = edge["node"].get("originalSrc")
            if url:
                all_images.append(url)

        # Category is just gender
        category_val = gender_tag.lower() if gender_tag else ""
        type_val=product.get("productType")


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
                if opt["name"].lower() == "colour":
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

def complete_workflow(collections):
    print("🔍 Scraping product IDs...")
    scraped_ids = scrape_main_product_ids(collections)
    unique_ids = list(set(scraped_ids))
    print(f"🎯 Total Unique Product IDs: {len(unique_ids)}")

    gids = format_shopify_gids(unique_ids)

    print("📦 Fetching product data in batches...")
    raw_data = fetch_shopify_products_batched(gids)

    all_products = []
    for entry in collections:
        gender_tag = entry.get("gender")
        product_type = entry.get("product_type")
        print(f"🧹 Cleaning data for {entry['url']}...")
        cleaned = clean_and_save_product_data_only_available_with_all_images_from_data(raw_data, gender_tag, product_type)
        all_products.extend(cleaned)

    # Remove duplicate products by handle (keep first occurrence)
    seen_handles = set()
    unique_products = []
    for prod in all_products:
        if prod["Handle"] not in seen_handles:
            unique_products.append(prod)
            seen_handles.add(prod["Handle"])

    # Write one JSON file
    # with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
    #     json.dump({"products": unique_products}, f, ensure_ascii=False, indent=4)
    # Upload all at once
    upsert_product({"products": unique_products}, BASE_URL, "USD")
    print(f"✅ Cleaned data saved to database and written to cleaned_products_new.json.")


# 🔧 Run Everything
if __name__ == "__main__":
    collections = [
        {"url": "https://thedesignerboxuk.com/en-us/collections/casablanca", "gender": "men"},
        {"url": "https://thedesignerboxuk.com/en-us/collections/sale", "gender": "men"}
    ]


    complete_workflow(collections)


