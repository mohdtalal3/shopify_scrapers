import requests
import json
import re
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import *
BASE_URL = "https://hypefly.co.in"
# --- Transform Function ---
def transform_products_with_description(products, gender_tag=None):
    cleaned_products = []

    for product in products:
        handle = product.get("slug", f'product-{product.get("id")}')
        if not handle:
            continue

        title = product.get("name", "")
        brand = product.get("brands")[0]["name"] if product.get("brands") else ""
        category_data = product.get("productCategory") or {}
        category = category_data.get("name", "")
        ptype = (product.get("productType") or {}).get("name", "")

        # Gender tags
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"mens", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}
            else:
                gender_tags = {"men", "women", "unisex", "shoes"}

        all_tags = [category, ptype] + list(gender_tags)
        tags_str = ','.join(sorted(set(filter(None, all_tags))))
        tags_str = tags_str + "," + brand

        # Collect image URLs
# Collect image URLs
        image_urls = []
        for image in product.get("images", []):
            if image.get("url"):
                image_urls.append(image["url"])
            formats = image.get("formats")
            if isinstance(formats, dict):
                for fmt in formats.values():
                    if fmt.get("url"):
                        image_urls.append(fmt["url"])
        image_urls = list(set(image_urls))

        # Full description
        raw_description = product.get("description", "").strip()
        body_html = f"<p>{brand} - {title}</p><p>{raw_description}</p>"

        product_data = {
            "Handle": handle,
            "Title": title,
            "Body (HTML)": body_html,
            "Vendor": brand,
            "Product Category": category.lower(),
            "Type": re.sub(r'\bwomens?\b', '', ptype, flags=re.IGNORECASE).strip(),
            "Tags": tags_str,
            "variants": []
        }

        # Add valid variants
        seen = set()
        for variant in product.get("variants", []):
            quantity = variant.get("quantity", 0)
            if not quantity or quantity <= 0:
                continue

            sku = str(variant.get("id"))
            size = variant.get("size", "")
            price = float(variant.get("salePrice", 0))
            ticket = float(variant.get("compareAtPrice") or price)

            if (size, sku) not in seen:
                product_data["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": "",
                    "Variant Price": price,
                    "Variant Compare At Price": ticket,
                    "images": image_urls
                })
                seen.add((size, sku))

        cleaned_products.append(product_data)

    return cleaned_products


# --- Fetch and Transform for Multiple Queries ---
def fetch_and_clean_all(search_terms):
    url = "https://meili.hypefly.co.in/multi-search"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 2b089e1ba60034cd6382c5c3c13f9d63519448983153abf6daebf3ea9031f25e"
    }

    all_cleaned = []

    for term in search_terms:
        print(f"🔍 Searching for: {term}")
        payload = {
            "queries": [
                {
                    "indexUid": "product",
                    "q": term,
                    "facets": [
                        "brands.name", "lowestPrice", "productCategory.name",
                        "productType.name", "variants.size"
                    ],
                    "filter": ["\"lowestPrice\">=0"],
                    "attributesToHighlight": ["*"],
                    "limit": 2000,
                    "offset": 0,
                    "sort": ["id:desc"]
                }
            ]
        }

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            products = data.get("results", [])[0].get("hits", [])
            cleaned = transform_products_with_description(products)
            all_cleaned.extend(cleaned)
        else:
            print(f"❌ Failed to fetch for '{term}': {response.status_code}")
            print(response.text)

    return all_cleaned

def complete_workflow_hypefly():
    search_terms = ["All Sneakers", "All Apparel", "Stanley","On Running"]

    cleaned_data = fetch_and_clean_all(search_terms)
    #upsert_all_product_data(cleaned_data, BASE_URL, "INR")
    
    output_file = "cleaned_products_from_multiple_terms.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2, ensure_ascii=False)

    print(f"✅ Total cleaned products saved: {len(cleaned_data)} to {output_file}")
    

if __name__ == "__main__":
    complete_workflow_hypefly()



