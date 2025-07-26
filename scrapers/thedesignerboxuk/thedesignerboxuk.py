import requests
import time
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import re
# BSS_PL.publicAccessToken = "d2e6ee62da9d5158adadada8c59c4bb1";
BASE_URL = "https://thedesignerboxuk.com"


graphql_url = "https://thedesignerbox.myshopify.com/api/2024-01/graphql.json"

headers = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://thedesignerboxuk.com",
    "Referer": "https://thedesignerboxuk.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "x-shopify-storefront-access-token": "d2e6ee62da9d5158adadada8c59c4bb1"
}
def extract_handle_from_url(url):
    import re
    match = re.search(r'/collections/([^/?#]+)', url)
    return match.group(1) if match else None


def fetch_product_ids_from_collection(url):
    collection_handle = extract_handle_from_url(url)
    print(collection_handle)
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

        response = requests.post(graphql_url, headers=headers, json=payload)
        data = response.json()

        edges = data["data"]["collectionByHandle"]["products"]["edges"]
        for edge in edges:
            gid = edge["node"]["id"]
            numeric_id = gid.split("/")[-1]
            all_ids.append(numeric_id)

        page_info = data["data"]["collectionByHandle"]["products"]["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        after_cursor = page_info["endCursor"]
    return all_ids
def format_shopify_gids(product_ids):
    return [f"gid://shopify/Product/{pid}" for pid in product_ids]


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
          media(first: 250) {
            edges {
              node {
                id
                alt
                previewImage { url id }
              }
            }
          }
          images(first: 250) {
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
            response = requests.post(graphql_url, headers=headers, json=payload)
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
    # # # # Save the results to a JSON file
    # with open("output.json", "w", encoding="utf-8") as f:
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
        brand = product.get("vendor")
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

      # Category is just gender
        category_val = gender_tag.lower() if gender_tag else ""
        type_val=product.get("productType")


        all_images = []
        for edge in product.get("images", {}).get("edges", []):
            url = edge["node"].get("originalSrc")
            if url:
                all_images.append(url)

        # Category is just gender
        category_val = gender_tag.lower() if gender_tag else ""
                # Use provided product_type if available
        type_val=product.get("productType")

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

def complete_workflow_thedesignerboxuk():

    collections = [
        {"url": "https://thedesignerboxuk.com/en-us/collections/casablanca", "gender": "men"},
        {"url": "https://thedesignerboxuk.com/en-us/collections/sale", "gender": "men"}
    ]
    print("🔍 Scraping product IDs from all collections...")
    all_scraped_ids = []
    product_id_to_collection = {}  # Map product IDs to their source collection
    
    # Scrape IDs from each collection URL
    for i, collection in enumerate(collections):
        print(f"→ Processing collection {i+1}/{len(collections)}: {collection['url']}")
        try:
            collection_ids = fetch_product_ids_from_collection(collection["url"])
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

    # # # # Write one JSON file
    # with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
    #     json.dump({"products": unique_products}, f, ensure_ascii=False, indent=4)
    # # Upload all at once
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


    complete_workflow_thedesignerboxuk()


