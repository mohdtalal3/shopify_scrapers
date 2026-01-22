import requests
import time
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import re
# shopify: {
#     staging: "gymshark-staging",
#     production: "gymshark"
# },
# storefront: {
#     staging: "60b46bd76dd1449a52b294c68961a3b2",
#     production: "b65646444b3639704a6ecb270cd28ce7"
# },

BASE_URL = "https://www.gymshark.com"

url = "https://gymsharkusa.myshopify.com/api/2025-01/graphql.json"
headers = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://www.gymshark.com",
    "Referer": "https://www.gymshark.com",
    "User-Agent": "Mozilla/5.0",
    "x-shopify-storefront-access-token": "527c7a1ab34f53ca4dc301924baee65d"
}



def format_shopify_gids(product_ids):
    return [f"gid://shopify/Product/{pid}" for pid in product_ids]

def fetch_product_ids_from_collection(collection_handle):
    all_ids = []
    has_next_page = True
    after_cursor = None

    while has_next_page:
        query = """
        query ($handle: String!, $cursor: String) {
          collectionByHandle(handle: $handle) {
            products(first: 250, after: $cursor) {
              pageInfo {
                hasNextPage
                endCursor
              }
              edges {
                node {
                  id
                  availableForSale
                }
              }
            }
          }
        }
        """

        variables = {
            "handle": collection_handle,
            "cursor": after_cursor
        }

        payload = {
            "query": query,
            "variables": variables
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        edges = data["data"]["collectionByHandle"]["products"]["edges"]
        for edge in edges:
            node = edge["node"]
            if node.get("availableForSale", True):
                gid = edge["node"]["id"]
                numeric_id = gid.split("/")[-1]
                all_ids.append(numeric_id)

        page_info = data["data"]["collectionByHandle"]["products"]["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        after_cursor = page_info["endCursor"]
        
    return all_ids


def fetch_shopify_products_batched(product_ids):
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
          images(first: 100) {
            edges {
              node {
                id
                originalSrc
                transformedSrc(maxWidth: 800, maxHeight: 800, crop: CENTER)
              }
            }
          }
          variants(first: 250) {
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
    """  # omitted for brevity (use your full query here)
    all_responses = {"data": {"nodes": []}}

    for i in range(0, len(product_ids), 250):
        batch = product_ids[i:i+250]
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
                print(f"[✓] Batch {i//250+1} fetched")
            else:
                print(f"[✗] Failed batch {i//250+1}: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f"[!] Exception in batch {i//250+1}: {e}")
        time.sleep(1.2)
    # # Save the results to a JSON file
    # with open("output.json", "w", encoding="utf-8") as f:
    #     json.dump(all_responses, f, ensure_ascii=False, indent=4)
    return all_responses

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
        product_tags = list(set(product.get("tags", [])))

        # Detect gender from product tags (check womens first to avoid substring match)
        gender_detected = None
        for tag in product_tags:
            tag_lower = tag.lower()
            if "womens" in tag_lower or "women's" in tag_lower:
                gender_detected = "women"
                break
            elif "mens" in tag_lower or "men's" in tag_lower:
                gender_detected = "men"
                break

        all_images = []
        seen_images = set()
        for edge in product.get("images", {}).get("edges", []):
            url = edge["node"].get("originalSrc")
            if url and url not in seen_images:
                all_images.append(url)
            seen_images.add(url)

        # Category is just gender
        category_val = gender_detected.lower() if gender_detected else ""
        type_val = product.get("productType")
        type_val = re.sub(r'\bwomens\b', '', type_val, flags=re.IGNORECASE).strip()
        type_val = re.sub(r'\bmens\b', '', type_val, flags=re.IGNORECASE).strip()
        
        gender_tags = set()
        if gender_detected:
            if gender_detected.lower() == "men":
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_detected.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}

        all_tags = product_tags + list(gender_tags)
        tags_str = ', '.join(sorted(all_tags))

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
                    "images": all_images
                })
                seen.add((size, sku))

    # Return as a list of product dicts
    return list(cleaned_products.values())



def complete_workflow_gymshark():
    # All collection handles (both men's and women's)
    all_collections = [
        # Women's collections
        "bottoms-leggings", "crop-tops", "hoodies-jackets", "shorts", "sports-bras", 
        "t-shirts-tops", "all-in-one", "dress", "vests", "skorts",
        # Men's collections
        "stringers", "base-layers", "bottoms", "tanks", "tracksuits"
    ]

    # Scrape all collections
    print("🔍 Scraping all collections...")
    all_ids = []
    for collection_handle in all_collections:
        print(f"→ Processing collection: {collection_handle}")
        try:
            collection_ids = fetch_product_ids_from_collection(collection_handle)
            all_ids.extend(collection_ids)
            print(f"✓ Found {len(collection_ids)} products from {collection_handle}")
        except Exception as e:
            print(f"✗ Error scraping {collection_handle}: {e}")
            continue
    
    unique_ids = list(set(all_ids))  # Remove duplicates
    print(f"🎯 Total Unique Product IDs: {len(unique_ids)}")

    if not unique_ids:
        print("❌ No product IDs found. Exiting.")
        return

    gids = format_shopify_gids(unique_ids)
    print("📦 Fetching product data in batches...")
    raw_data = fetch_shopify_products_batched(gids)
    
    # Clean and process products (gender will be detected from tags)
    all_products = clean_and_save_product_data_only_available_with_all_images_from_data(raw_data)

    if not all_products:
        print("❌ No products found. Exiting.")
        return

    # Remove duplicate products by handle (keep first occurrence)
    seen_handles = set()
    unique_products = []
    for prod in all_products:
        if prod["Handle"] not in seen_handles:
            unique_products.append(prod)
            seen_handles.add(prod["Handle"])

    # Save to JSON if needed
    with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
        json.dump({"products": unique_products}, f, ensure_ascii=False, indent=4)

    # Unified upsert for products, tags, and colors
    upsert_all_product_data(unique_products, BASE_URL, "USD")
    print(f"📊 Total unique products processed: {len(unique_products)}")


# 🔧 Run Everything
if __name__ == "__main__":


    complete_workflow_gymshark()
    
    # Alternative: Extract colors from existing JSON file
    # extract_and_update_colors_from_json("cleaned_products_new.json")


