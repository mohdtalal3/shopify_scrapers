import requests
import json
import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import *

BASE_URL = "www.toryburch.com"

def clean_and_save_toryburch_product_data(
    raw_data, gender_tag=None, product_type=None, out_file="cleaned_products.json"
):
    products = raw_data.get("products", [])
    cleaned_products = {}

    for product in products:
        if product is None:
            continue
        product_id = product.get("id")
        title = product.get("name")
        description = f"<p>{product.get('nameInternational') or ''}</p>"
        brand = product.get("brand", "Tory Burch")
        product_tags = list(set(filter(None, [
            product.get("productDepartmentName", ""),
            product.get("productClassName", ""),
            product.get("productFamilyId", "")
        ])))
        category_val = gender_tag.lower() if gender_tag else ""
        type_val = product.get("productClassName", "").strip()
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"womens", "women"}
            else:
                gender_tags = {"men", "women", "unisex", "shoes", "unisex"}
        all_tags = sorted(set(product_tags + list(gender_tags)))
        tags_str = ", ".join(all_tags)
        if product_id not in cleaned_products:
            cleaned_products[product_id] = {
                "Handle": product_id,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category_val,
                "Type": type_val,
                "Tags": tags_str,
                "variants": [],
                "seen_fingerprints": set()
            }
        seen_fingerprints = cleaned_products[product_id]["seen_fingerprints"]
        for swatch in product.get("swatches", []):
            color = swatch.get("colorName", "")
            sku = swatch.get("_id", "")
            price = float(swatch.get("price", {}).get("min", 0))
            compare_price = float(swatch.get("price", {}).get("max", 0))
            images = [
                f"https://s7.toryburch.com/is/image/ToryBurch/style/{img}.pdp-1534x1744.jpg"
                for img in swatch.get("images", [])
            ]
            for size_info in product.get("sizes", []):
                size = size_info.get("value", "")
                variant_obj = {
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": price,
                    "Variant Compare At Price": compare_price,
                    "images": images
                }
                fingerprint = json.dumps(variant_obj, sort_keys=True)
                if fingerprint in seen_fingerprints:
                    continue
                cleaned_products[product_id]["variants"].append(variant_obj)
                seen_fingerprints.add(fingerprint)
    cleaned_list = []
    for product_data in cleaned_products.values():
        product_data.pop("seen_fingerprints", None)
        cleaned_list.append(product_data)
    # with open("cleaned_products_new.json", "w", encoding="utf-8") as f:
    #     json.dump({"products": cleaned_list}, f, ensure_ascii=False, indent=4)
    upsert_all_product_data(cleaned_list, BASE_URL, "USD")
    return cleaned_list

# --- Main logic: fetch per department, merge, clean ---

def fetch_products_for_department(department):
    url = "https://www.toryburch.com/api/prod-r2/v11/categories/sale-view-all/products"
    params = {
        "site": "ToryBurch_US",
        "locale": "en-us",
        "pip": "true",
        "limit": "200",
        "layout": "flex",
        "filter[c_productDepartment]": department
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "x-api-key": "yP6bAmceig0QmrXzGfx3IG867h5jKkAs",
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data.get("products", [])
    else:
        print(f"❌ Failed to fetch data for {department}. Status code: {response.status_code}")
        return []

def complete_workflow_tory():
    departments = ["Accessories", "Shoes", "Handbags", "Wallets"]
    #departments = ["Accessories"]
    all_products = []
    for dept in departments:
        print(f"Fetching products for department: {dept}")
        products = fetch_products_for_department(dept)
        all_products.extend(products)
    # Remove duplicates by product id
    unique_products = {}
    for product in all_products:
        if product and product.get("id") not in unique_products:
            unique_products[product.get("id")] = product
    merged_data = {"products": list(unique_products.values())}
    clean_and_save_toryburch_product_data(merged_data, gender_tag="women")

if __name__ == "__main__":
    complete_workflow_tory()


# andrealago