import requests
import time
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
# from dotenv import load_dotenv
# load_dotenv()
# proxy_str = os.getenv("PROXY_URL")

# # Proxies dictionary for requests
# proxies = {
#     "http": proxy_str,
#     "https": proxy_str
#}
#https://${p}/api/unstable/graphql.json`

        # <script id="shopify-features" type="application/json">
        #     {
        #         "accessToken": "3260355354f75aae395e213ca40bf675",
        #         "betas": [
        #             "rich-media-storefront-analytics"
        #         ],


BASE_URL = "https://shop_whoop.com"

def extract_product_data(node, parent_info=None):
    """
    Recursively extracts product data from the nested JSON structure.
    """
    products = []
    
    product_info = node.get("product_info", {})
    
    # Only process nodes that represent actual products with SKUs,
    # or parent products that might contain overall descriptions/images.
    # We will primarily be interested in nodes that have 'items' (variants).
    is_variant_parent = bool(product_info.get("items")) or product_info.get("product_type") == "Parent Product"

    if is_variant_parent or node.get("children"): # Process if it's a product or has children
        handle = product_info.get("handle")
        title = product_info.get("title")
        description = product_info.get("description", "")
        vendor = "WHOOP" # Based on the data, hardcoding vendor as WHOOP

        product_type = product_info.get("product_type", "").replace("Parent Product", "").replace("Category", "").strip()
        
        # Clean product type by removing gendered terms
        gender_terms = ["Men's", "Women's", "Girls'", "Boys'"]
        for term in gender_terms:
            if term in title:
                title = title.replace(term, "").strip()
            if term in product_type:
                product_type = product_type.replace(term, "").strip()

        # Product category logic
        product_category = ""
        if product_info.get("category_label"):
            product_category = product_info.get("category_label")
        elif product_info.get("product_type") and product_info.get("product_type") != "Parent Product":
             product_category = product_info.get("product_type")

        tags_set = set()
        # Add colors as tags
        colors = product_info.get("colors")
        if colors:
            for color in colors:
                if color and color.get("label"):
                    tags_set.add(color["label"].lower())
        # Add sizes as tags
        sizes = product_info.get("sizes")
        if sizes:
            for size in sizes:
                if size and size.get("label"):
                    tags_set.add(size["label"].lower())
        # Add product highlights as tags
        highlights = product_info.get("product_highlights")
        if highlights:
            for highlight in highlights:
                if highlight:
                    tags_set.add(highlight.lower().replace("_", " "))
        
        tags_set.add("unisex,men,women")

        # Add product type and category as tags
        if product_type:
            tags_set.add(product_type.lower())
        if product_category:
            tags_set.add(product_category.lower())


        tags = ', '.join(sorted([t.strip() for t in tags_set if t.strip()]))

        # Collect all images from the current product_info
        all_images = []
        featured_media = product_info.get("featured_media")
        if featured_media and featured_media.get("url"):
            all_images.append(featured_media["url"])
        media = product_info.get("media")
        if media:
            for media_item in media:
                if media_item and media_item.get("url") and media_item.get("type") == "image":
                    all_images.append(media_item["url"])
        # all_images = list(set(all_images))  # Removed - order preserved during collection # Remove duplicates

        current_product = {
            "Handle": handle,
            "Title": title,
            "Body (HTML)": description,
            "Vendor": vendor,
            "Product Category": product_category,
            "Type": product_type,
            "Tags": tags,
            "variants": []
        }

        # Process variants (items) for the current product
        items = product_info.get("items")
        if items:
            for item in items:
                if not item:
                    continue
                    
                sku = item.get("sku", "")
                
                # Find the USD price if available, otherwise take the first price
                sale_price = 0
                list_price = 0
                
                prices = item.get("prices")
                usd_price_found = False
                if prices:
                    for price_entry in prices:
                        if price_entry and price_entry.get("currency") == "usd":
                            sale_price = price_entry.get("amount", 0)
                            list_price = price_entry.get("sale_amount", 0) or price_entry.get("pro_sale_amount", 0)
                            usd_price_found = True
                            break
                    
                    if not usd_price_found and prices: # If USD not found, take the first available price
                        first_price = prices[0]
                        if first_price:
                            sale_price = first_price.get("amount", 0)
                            list_price = first_price.get("sale_amount", 0) or first_price.get("pro_sale_amount", 0)


                # Determine if in stock based on inventory
                is_in_stock = False
                inventory = item.get("inventory")
                if inventory:
                    for inv_item in inventory:
                        if inv_item and inv_item.get("is_active") and inv_item.get("quantity", 0) > 0:
                            is_in_stock = True
                            break
            
                if not is_in_stock and not product_info.get("join_flow"): # Only include if in stock or part of a join flow
                    continue # Skip out-of-stock items if not join_flow

                size_obj = item.get("size", {})
                size = size_obj.get("label", "OS") if size_obj else ""
                color_obj = item.get("color", {})
                color = color_obj.get("label", "") if color_obj else ""

                variant_images = []
                media = item.get("media")
                if media:
                    for media_item in media:
                        if media_item and media_item.get("url") and media_item.get("type") == "image":
                            variant_images.append(media_item["url"])
                
                # Fallback to product images if no specific variant images
                if not variant_images:
                    variant_images = all_images

                current_product["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": float(sale_price / 100), # Assuming amounts are in cents/smallest unit
                    "Variant Compare At Price": float(list_price / 100) if list_price else 0.0,
                    "images": variant_images
                })
        
        # Add the current product to the list if it has variants or is a main parent
        if current_product["variants"] or product_info.get("product_type") == "Parent Product":
            products.append(current_product)

    # Recursively process children
    for child in node.get("children", []):
        products.extend(extract_product_data(child, product_info))
        
    return products

graphql_url = "https://whoop-inc.myshopify.com/api/2025-01/graphql"
headers = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://shop.whoop.com",
    "Referer": "https://shop.whoop.com/",
    "User-Agent": "Mozilla/5.0",
    "x-shopify-storefront-access-token": "1f2a0558b60b316050f218b4bf11a963"
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
                  title
                  handle
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
        # with open("output.json", "w", encoding="utf-8") as f:
        #     json.dump(data, f, ensure_ascii=False, indent=4)
        edges = data["data"]["collectionByHandle"]["products"]["edges"]
        for edge in edges:
            gid = edge["node"]["handle"]
            all_ids.append(gid)

        page_info = data["data"]["collectionByHandle"]["products"]["pageInfo"]
        has_next_page = page_info["hasNextPage"]
        after_cursor = page_info["endCursor"]
    # with open("all_ids.json", "w", encoding="utf-8") as f:
    #     json.dump(all_ids, f, ensure_ascii=False, indent=4)
    return all_ids

def fetch_all_products(handles):
    main_url = "https://api.prod.whoop.com/product-service/v2/product/"

    params = {
        "language": "en",
        "channel": "storefront",
        "currency": "USD"
    }

    all_records = []
    for handle in handles:
        print(handle)
        url = f"{main_url}{handle}"
        print(url)
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            records = extract_product_data(data)
            all_records.extend(records)
        else:
            print(f"Error {response.status_code} for handle {handle}: {response.text}")

    return all_records



def complete_workflow_shop_whoop():

    collections = [
        {"url": "https://shop.whoop.com/en-us/collections/mg-bands/", "gender": "men"}
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


    print("📦 Fetching product data in batches...")
    raw_data = fetch_all_products(unique_ids)

    # Remove duplicate products by handle (keep first occurrence)
    seen_handles = set()
    unique_products = []
    for prod in raw_data:
        if prod["Handle"] not in seen_handles:
            unique_products.append(prod)
            seen_handles.add(prod["Handle"])

    # # # # # Write one JSON file
    # with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
    #     json.dump({"products": unique_products}, f, ensure_ascii=False, indent=4)
    # # Upload all at once
    upsert_all_product_data(unique_products, BASE_URL, "USD")
    print(f"✅ Cleaned data saved to database and written to cleaned_products_new.json.")
    print(f"📊 Total unique products processed: {len(unique_products)}")
    

# 🔧 Run Everything
if __name__ == "__main__":


    complete_workflow_shop_whoop()


