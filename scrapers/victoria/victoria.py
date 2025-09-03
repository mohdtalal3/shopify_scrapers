import requests
import json
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

# ------------------ CONFIG ------------------
STACKS_URL = "https://api.victoriassecret.com/stacks/v40/"
PRODUCT_URL = "https://api.victoriassecret.com/products/v37/page/1127128500"
OUTPUT_FILE = "all_products_cleaned.json"
BASE_URL="https://www.victoriassecret.com"
MAX_WORKERS = 10
TIMEOUT = 15
LIMIT = 180  # batch size for pagination

STACKS_PARAMS = {
    "brand": "vs",
    "collectionId": "9f1251d2-73ed-4294-a159-d3fbcdbd7577",
    "orderBy": "REC",
    "maxSwatches": 8,
    "isPersonalized": "true",
    "isWishlistEnabled": "true",
    "activeCountry": "US",
    "platform": "web",
    "perzConsent": "true",
    "tntId": "90aa54bf-bea7-4cb6-a345-7a6e83bd2fcb.34_0",
    "screenWidth": 1920,
    "screenHeight": 1080
}

# ------------------ LOAD PROXY ------------------
load_dotenv()
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None

# ------------------ HELPER: FORMAT IMAGE URL ------------------
def format_vs_image_url(original_url: str) -> str:
    """
    Given an image URL from the API, attempt to extract an ID or filename
    and construct a matching VS 1520x2026 JPG image URL.
    """
    # Example assumption: original_url ends with something like ".../abc123.jpeg" or similar
    return f"https://www.victoriassecret.com/p/1520x2026/{original_url}.jpg"

# ------------------ CLEAN FUNCTION ------------------
def clean_and_save_product_data_only_available_with_all_images_from_data(
    data, gender_tag=None, product_type=None
):
    product = data.get("product", {})
    if not product:
        return []

    cleaned_products = {}
    generic_id = product.get("featuredChoice", {}).get("genericId", "")
    if not generic_id or generic_id not in product.get("productData", {}):
        return []

    handle = product.get("id", "")
    title = product.get("shortDescription", "")
    description = product.get("productData", {}).get(generic_id, {}).get("longDescription", "")
    brand = product.get("brandName", "")
    product_type_val = product.get("classDisplay", "")
    category_val = product.get("categoryDisplay", "")

    product_tags = product.get("itemLevelCallout", [])
    top_level_brand = product.get("topLevelBrand", "")
    if top_level_brand:
        product_tags.append(top_level_brand)

    gender_tags = set()
    if gender_tag:
        if gender_tag.lower() == "men":
            gender_tags = {"all clothing men", "mens", "men clothing", "men"}
        elif gender_tag.lower() == "women":
            gender_tags = {"all clothing women", "womens", "women clothing", "women"}

    all_tags = product_tags + list(gender_tags)
    if product_type:
        all_tags.extend(product_type.split())
    if product_type_val:
        all_tags.extend(product_type_val.split())
    product_tags = ", ".join(tag.strip() for tag in set(all_tags) if tag.strip())

    cleaned_products[handle] = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": description,
        "Vendor": brand,
        "Product Category": category_val,
        "Type": product_type_val,
        "Tags": product_tags,
        "variants": []
    }

    seen = set()
    for choice_data in product.get("productData", {}).get(generic_id, {}).get("choices", {}).values():
        color = choice_data.get("color", "")
        all_images = []
        seen_images = set()
        for image_data in choice_data.get("images", []):
            original_url = image_data.get("image", "")
            if not original_url or original_url in seen_images:
                continue
            seen_images.add(original_url)
            formatted = format_vs_image_url(original_url)
            all_images.append(formatted)

        for size_data in choice_data.get("availableSizes", {}).values():
            if not size_data.get("isAvailable", False):
                continue

            sku = size_data.get("variantId", "")
            price = float(size_data.get("originalPriceNumerical", 0))
            compare_price = float(size_data.get("salePriceNumerical", 0)) if size_data.get("salePriceNumerical") else 0
            size = size_data.get("size1", "")

            key = (size, sku)
            if key not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": price,
                    "Variant Compare At Price": compare_price,
                    "images": all_images
                })
                seen.add(key)

    return list(cleaned_products.values())

# ------------------ FETCH PRODUCT ------------------
def fetch_product(pid, gender_tag="women", product_type="lingerie"):
    try:
        resp = requests.get(
            PRODUCT_URL,
            params={"productId": pid, "priceType": "regular", "activeCountry": "US"},
            proxies=proxies,
            timeout=TIMEOUT
        )
        resp.raise_for_status()
        product_json = resp.json()
        return clean_and_save_product_data_only_available_with_all_images_from_data(
            product_json, gender_tag=gender_tag, product_type=product_type
        )
    except Exception as e:
        print(f"   ⚠️ Error for product {pid}: {e}")
        return []

# ------------------ FETCH ALL PRODUCT IDS ------------------
def fetch_all_product_ids():
    product_ids = []
    resp = requests.get(STACKS_URL, params=STACKS_PARAMS, proxies=proxies, timeout=TIMEOUT)
    if resp.status_code != 200:
        print(f"❌ Failed fetching stacks: {resp.status_code}")
        return []

    data = resp.json()
    total_items = data.get("TotalItems", 0)
    print(f"📦 Total items reported: {total_items}")

    for stack in data.get("stacks", []):
        for item in stack.get("list", []):
            if "id" in item:
                product_ids.append(item["id"])

    stack_id = data["stacks"][0]["id"] if data.get("stacks") else None
    if not stack_id:
        print("❌ No stackId found.")
        return product_ids

    offset = len(product_ids)
    while offset < total_items:
        params = {
            "collectionId": STACKS_PARAMS["collectionId"],
            "stackId": stack_id,
            "maxSwatches": 8,
            "isPersonalized": "true",
            "isWishlistEnabled": "true",
            "perzConsent": "true",
            "platform": "web",
            "orderBy": "REC",
            "offset": offset,
            "limit": LIMIT,
            "activeCountry": "US",
            "screenWidth": 1920,
            "screenHeight": 1080,
            "tntId": STACKS_PARAMS["tntId"]
        }
        resp = requests.get(STACKS_URL + "stack", params=params, proxies=proxies, timeout=TIMEOUT)
        if resp.status_code != 200:
            print(f"❌ Failed at offset {offset}: {resp.status_code}")
            break
        page = resp.json()
        # Handle both list and dict responses
        if isinstance(page, list):
            items = page
        elif isinstance(page, dict):
            items = page.get("list", [])
        else:
            items = []
            
        for item in items:
            if "id" in item:
                product_ids.append(item["id"])
        print(f"   ✔ Fetched {len(product_ids)} / {total_items}")
        offset += LIMIT

    return product_ids

# ------------------ MAIN ------------------
def complete_workflow_victoria():
    product_ids = fetch_all_product_ids()
    print(f"✅ Extracted {len(product_ids)} product IDs")

    all_cleaned = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_product, pid): pid for pid in product_ids}
        for future in as_completed(futures):
            pid = futures[future]
            result = future.result()
            if result:
                all_cleaned.extend(result)
                print(f"   ✔ Processed {pid}")
    upsert_all_product_data(all_cleaned, BASE_URL, "USD")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_cleaned, f, indent=2)

    print(f"🎉 Done! Saved {len(all_cleaned)} cleaned products to {OUTPUT_FILE}")

if __name__ == "__main__":
    complete_workflow_victoria()
