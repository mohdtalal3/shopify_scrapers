import requests
import time
import json
import re
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_product



BASE_URL = "https://shop437.com"

def format_shopify_gids(product_ids):
    return [f"gid://shopify/Product/{pid}" for pid in product_ids]



def get_total_pages(soup):
    """
    Parse the total number of pages from the pagination block
    """
    pages = soup.select("ul.pagination__list li a")
    page_numbers = []
    for page in pages:
        text = page.get_text(strip=True)
        if text.isdigit():
            page_numbers.append(int(text))
    return max(page_numbers) if page_numbers else 1

def extract_product_handles_from_soup(soup):
    """
    Extract product handles from a BeautifulSoup object
    """
    product_links = soup.select('a[href*="/products/"]')
    handles = set()
    for link in product_links:
        href = link.get("href")
        if href:
            match = re.search(r'/products/([^/?]+)', href)
            if match:
                handles.add(match.group(1))
    return handles

def get_product_id_from_handle(handle):
    """
    Query the .js endpoint to get the numeric product id
    """
    url = f"https://shop437.com/products/{handle}.js"
    try:
        response = requests.get(url)
        response.raise_for_status()
        product_json = response.json()
        return product_json.get("id")
    except requests.exceptions.RequestException as e:
        print(f"Error loading {handle}: {e}")
        return None

def scrape_all_product_ids(collection_url):
    all_handles = set()
    
    # Fetch first page
    try:
        response = requests.get(collection_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    
    # find total pages
    total_pages = get_total_pages(soup)
    print(f"Total pages: {total_pages}")
    
    # collect handles from first page
    all_handles.update(extract_product_handles_from_soup(soup))
    
    # loop over the rest
    for page in range(2, total_pages + 1):
        page_url = f"{collection_url}?page={page}"
        try:
            resp = requests.get(page_url)
            resp.raise_for_status()
            page_soup = BeautifulSoup(resp.text, "html.parser")
            all_handles.update(extract_product_handles_from_soup(page_soup))
            print(f"Scraped page {page}")
        except requests.exceptions.RequestException as e:
            print(f"Failed page {page}: {e}")
            continue
    
    print(f"Total unique handles found: {len(all_handles)}")

    # now get numeric product IDs
    product_ids = []
    for handle in all_handles:
        product_id = get_product_id_from_handle(handle)
        if product_id:
            product_ids.append(product_id)

    return product_ids


def fetch_shopify_products_batched(product_ids):
    url = "https://437swim.myshopify.com/api/2025-04/graphql.json"

    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://shop437.com",
        "Referer": "https://shop437.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-shopify-storefront-access-token": "c27db60e5d6b9e3a32cef60f40b532c3"
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
        title_formatted=""
        if title.startswith("The "):
            title_formatted = title[4:]

        # remove anything after the first slash
        title_formatted = title_formatted.split("/")[0].strip()
        # N-grams from last 3 words of title
        title_formatted = re.sub(r'\b\d+\s*Pack\b', '', title_formatted, flags=re.IGNORECASE).strip()
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
        # Use provided product_type if available
        if title_formatted == "":
            type_val=product.get("productType")
        else:
            words = title_formatted.split()
            type_val = words[-1] if words else ""

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

def complete_workflow(collections):
    print("🔍 Scraping product IDs...")
    scraped_ids = scrape_all_product_ids(collections[0]["url"])
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
        {"url": "https://shop437.com/collections/shop-all", "gender": "women"},
    ]

    complete_workflow(collections)


