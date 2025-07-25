import requests
import json
import re
import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import *
BASE_URL = "https://sportsdirect.com"
def fetch_all_sportsdirect_products():
    base_url = "https://us.sportsdirect.com/product/getforcategory"
    base_params = {
        "categoryId": "SDUS_SALEASICSMENS",
        "productsPerPage": 60,
        "sortOption": "rank",
        "selectedFilters": "CATG^Trainers",
        "isSearch": "false",
        "searchText": "",
        "columns": 3,
        "mobileColumns": 2,
        "clearFilters": "false",
        "pathName": "/sale/asics/mens",
        "searchTermCategory": "",
        "selectedCurrency": "USD",
        "portalSiteId": 193,
        "searchCategory": ""
    }
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    all_products = []
    page = 1
    total_pages = None

    while True:
        print(f"Fetching page {page}...")
        params = base_params.copy()
        params["page"] = page

        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break

        data = response.json()

        # Append products
        products = data.get("products", [])
        all_products.extend(products)
        print(len(all_products))
        # Set total_pages from first response
        if total_pages is None:
            total_pages = data.get("numberOfPages", 1)

        # Break if this was the last page
        if page >= total_pages:
            break

        page += 1
        time.sleep(1)  # Politeness delay
    
    return all_products

import re
def scale_variant(api_variant, known_api_selling_price, known_actual_selling_price):
    factor = known_actual_selling_price / known_api_selling_price
    return round(api_variant["ticketPrice"] * factor, 2),round(api_variant["sellingPrice"] * factor, 2)


def clean_flat_sportsdirect_data(products, gender_tag=None):
    cleaned_products = {}

    for product in products:
        # Use URL as handle (remove leading slash), fallback to productId
        raw_url = product.get("url", "").strip()
        handle = raw_url.strip("/") if raw_url else product.get("productId", "")

        if not handle:
            continue  # Skip if no handle can be found

        title = product.get("name")
        description = f"<p>{product.get('brand', '')} - {product.get('name', '')}</p>"
        brand = product.get("brand", "")
        product_tags = [product.get("category", ""), product.get("subCategory", "")]

        category_val = product.get("category", "")
        type_val = product.get("subCategory", "")

        # Gender tags
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"mens", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}
            else:
                gender_tags = {"men", "women", "unisex", "shoes"}

        all_tags = product_tags + list(gender_tags)
        tags_str = ', '.join(sorted(set(filter(None, all_tags))))

        # Images
        all_images = []
        for key in ["image", "imageLarge", "alternativeImage", "alternativeImageLarge"]:
            if product.get(key):
                all_images.append(product[key])

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category_val.lower(),
                "Type": re.sub(r'\bwomens?\b', '', type_val, flags=re.IGNORECASE).strip(),
                "Tags": tags_str,
                "variants": []
            }

        seen = set()
        actual_selling_price = float(product.get("priceUnFormatted", 0))
        api_price=float(product.get("priceInBaseUnit", 0))
        for variant in product.get("sizeVariants", []):
            sku = variant.get("variantId", "")
            ticket_price, selling_price = scale_variant(variant, api_price, actual_selling_price)

            size = variant.get("description", "")
            color = product.get("colourName", "")

            if (size, sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": selling_price,
                    "Variant Compare At Price": ticket_price,
                    "images": list(set(all_images))
                })
                seen.add((size, sku))

    return list(cleaned_products.values())




def complete_workflow_sportsdirect():
    all_pages = fetch_all_sportsdirect_products()
    cleaned_data = clean_flat_sportsdirect_data(all_pages, gender_tag="men")
    upsert_all_product_data(cleaned_data, BASE_URL, "USD")

# --- Main workflow ---
if __name__ == "__main__":
    all_pages = fetch_all_sportsdirect_products()
    cleaned_data = clean_flat_sportsdirect_data(all_pages, gender_tag="men")
    upsert_all_product_data(cleaned_data, BASE_URL, "USD")
    # with open("sportsdirect_asics_formatted.json", "w", encoding="utf-8") as f:
    #     json.dump(cleaned_data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(cleaned_data)} cleaned products to 'sportsdirect_asics_formatted.json'")
