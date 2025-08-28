import requests
import json
import math
import re
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import re



BASE_URL = "https://www.underarmour.com"

def fetch_all_products():
    """
    Fetches all products from the Under Armour outlet API by handling pagination.

    Returns:
        list: A list of all product results fetched from the API.
    """
    print("Starting product data extraction...")
    base_url = "https://ac.cnstrc.com/browse/group_id/outlet"
    params = {
        "c": "ciojs-client-2.65.0",
        "key": "key_Gz4VzKsXbR7b7fSh",
        "i": "773d2e1b-d558-4ea1-976e-712c4aad1471",
        "s": "3",
        "offset": "0",
        "num_results_per_page": "200",
        "filters[subsilhouette]": [
            "Short Sleeves", "Graphics", "Shorts", "Sneakers",
            "Sandals and Slides", "Polos", "Sleeveless", "Pants",
            "Hoodies and Sweatshirts", "Long Sleeves", "Leggings", "Gloves"
        ],
        "sort_by": "salePrice",
        "sort_order": "ascending",
    }

    all_products = []
    
    # --- Make the first request to get total number of products ---
    print("Fetching initial page to determine total products...")
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        
        total_results = data.get("response", {}).get("total_num_results", 0)
        if total_results == 0:
            print("No products found. Exiting.")
            return []
            
        print(f"Total products found: {total_results}")
        
        # Add products from the first page
        initial_products = data.get("response", {}).get("results", [])
        all_products.extend(initial_products)
        print(f"Fetched {len(initial_products)} products from page 1.")

        # --- Calculate pagination and fetch remaining pages ---
        num_per_page = int(params["num_results_per_page"])
        total_pages = math.ceil(total_results / num_per_page)
        print(f"Total pages to fetch: {total_pages}")

        for page in range(1, total_pages):
            offset = page * num_per_page
            params["offset"] = str(offset)
            
            print(f"Fetching page {page + 1} of {total_pages} (offset: {offset})...")
            
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            page_data = response.json()
            
            products_on_page = page_data.get("response", {}).get("results", [])
            if not products_on_page:
                print(f"No more products found on page {page + 1}. Stopping.")
                break
                
            all_products.extend(products_on_page)
            print(f"-> Fetched {len(products_on_page)} products. Total fetched so far: {len(all_products)}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
        return None
    except json.JSONDecodeError:
        print("Failed to decode JSON from the response.")
        return None

    print(f"\nFinished fetching. Total products gathered: {len(all_products)}")
    return all_products


def format_under_armour_data(raw_products_list):
    """
    Cleans and formats a list of product data, ensuring each variant has its own specific images.

    Args:
        raw_products_list (list): The list of raw product dictionaries from the API.

    Returns:
        list: A list of dictionaries, where each dictionary represents a cleaned product
              with its variants and their specific images.
    """
    print("\nStarting data cleaning and formatting...")
    cleaned_products = {}

    for product in raw_products_list:
        product_data = product.get("data")
        if not product_data or not product_data.get("orderable"):
            continue

        handle = product_data.get("id")
        if not handle:
            continue

        # Determine product category (gender) from the subHeader
        sub_header = product_data.get("subHeader", "").lower()
        category_val = ""
        if "men's" in sub_header:
            category_val = "men"
        elif "women's" in sub_header:
            category_val = "women"
        elif "boys'" in sub_header:
            category_val = "men"
        elif "girls'" in sub_header:
            category_val = "women"
        
        clothing_keywords = [
            # Generic
            "all clothing",
            "t-shirts", "shirt", "shirts", "tee", "tees",
            "polo", "polo shirts",
            "dress", "dresses", "skort", "skorts",
            "jean", "jeans",
            "jacket", "jackets", "vest", "vests",
            "coat", "coats",
            "blouse", "blouses",
            "swim", "swimwear", "beachwear",
            "sweatshirt", "sweatshirts", "hoodie", "hoodies",
            "knitwear", "knit",
            "skirt", "skirts",
            "pant", "pants", "legging", "leggings",
            "suit", "suits",
            "underwear", "bra", "bras",
            "lounge", "sleepwear", "pajama", "pajamas",
            "sportswear", "activewear", "baselayer",
            "short", "shorts",
            "matching", "set", "sets",
            "shoe", "shoes",
            "outerwear",
            "sports bra",
            "underwear",
            "swim", "swimwear",
        ]


        def is_clothing_type(ptype: str) -> bool:
            """Check if productType partially matches any clothing keyword."""
            for keyword in clothing_keywords:
                if keyword.replace("&", "").replace("-", "").replace(" ", "") in ptype.replace("&", "").replace("-", "").replace(" ", ""):
                    return True
                if keyword in ptype:
                    return True
            return False
        gender_tags = set()
        gender_tag=category_val
        if gender_tag:
            gender = gender_tag.lower()
            clothing_match = is_clothing_type(sub_header)
            if gender == "men":
                if clothing_match:
                    gender_tags = {"all clothing men", "mens", "men clothing", "men","men's"}
                else:
                    gender_tags = {"mens", "men","men's"}
            elif gender == "women":
                if clothing_match:
                    gender_tags = {"all clothing women", "womens", "women clothing", "women","women's"}
                else:
                    gender_tags = {"womens", "women","women's"}


        # Combine facets to create tags
        tags_list = []
        for facet in product_data.get("facets", []):
            facet_name = facet.get("name")
            for value in facet.get("values", []):
                tags_list.append(f"{facet_name}: {value}")
        type_val=sub_header
        cleaned_type = re.sub(
            r"\b(men|mens|women|womens|woman|girl|girls|boy|boys)(?:'s|s')?\b",
            "",
            type_val,
            flags=re.IGNORECASE
        )
        # Remove extra spaces
        cleaned_type = re.sub(r"\s+", " ", cleaned_type).strip()
        all_tags = tags_list + list(gender_tags) 
        if cleaned_type:
            all_tags.append(cleaned_type)
            
        tags_string = ", ".join(tag for tag in all_tags if tag)

# Remove extra spaces caused by removals
        cleaned_type = re.sub(r"\s+", " ", cleaned_type).strip()
        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": product.get("value", ""),
                "Body (HTML)": f"<p>{product_data.get('description', '')}</p>",
                "Vendor": "Under Armour",
                "Product Category": category_val,
                "Type": cleaned_type,
                "Tags": tags_string,
                "variants": []
            }

        seen_variants = set()
        for variant_edge in product.get("variations", []):
            variant = variant_edge.get("data", {})
            if not variant.get("orderable"):
                continue

            sku = variant.get("sku")
            size = ""
            for facet in variant.get("facets", []):
                if facet.get("name", "").lower() == "size":
                    size = facet.get("values", [""])[0]
                    break
            
            variant_images = []
            if variant.get("image_url"):
                variant_images.append(variant["image_url"])
            if variant.get("gridTileHoverImageURL"):
                variant_images.append(variant["gridTileHoverImageURL"])

            if sku and (sku, size) not in seen_variants:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": variant.get("colorValue", ""),
                    "Variant Price": float(variant.get("salePrice", 0)),
                    "Variant Compare At Price": float(variant.get("listPrice", 0)),
                    "images": variant_images 
                })
                seen_variants.add((sku, size))

    print(f"Formatting complete. Cleaned {len(cleaned_products)} unique products.")
    return list(cleaned_products.values())


def complete_workflow_underarmour():
    all_product_results = fetch_all_products()

    if all_product_results:
        # 2. Clean and format the fetched data
        formatted_data = format_under_armour_data(all_product_results)
        upsert_all_product_data(formatted_data, BASE_URL, "USD")
        print(f"Upserted {len(formatted_data)} products to {BASE_URL}")

# --- Main execution ---
if __name__ == "__main__":
    complete_workflow_underarmour()

        # # 3. Save the newly formatted data to a file
        # output_filename = 'formatted_products_corrected.json'
        # try:
        #     with open(output_filename, 'w', encoding="utf-8") as f:
        #         json.dump(formatted_data, f, indent=4, ensure_ascii=False)
            
        #     print(f"\n✅ Data has been successfully fetched, formatted, and saved.")
        #     print(f"Saved to '{output_filename}'")

        # except IOError as e:
        #     print(f"Error saving the file: {e}")

