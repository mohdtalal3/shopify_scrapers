from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import os
import re
import sys
import time

from dotenv import load_dotenv
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from db import upsert_all_product_data

# BSS_PL.publicAccessToken = "d2e6ee62da9d5158adadada8c59c4bb1";
BASE_URL = "https://www.katespade.com"
load_dotenv()

proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None

# ==============================
# Utility functions
# ==============================

def extract_product_id_from_url(product_url: str) -> str:
    """Extract product ID like CBI17 from product URL"""
    if not product_url:
        return ""
    try:
        last_part = product_url.split("/")[-1]
        base = last_part.split(".html")[0]
        return base.split("-")[0]
    except Exception:
        return ""

def clean_and_extract_product_ids(data):
    """Extract product IDs from get-shop response"""
    products = data.get("pageData", {}).get("products", [])
    ids = []
    for product in products:
        if not product:
            continue
        pid = extract_product_id_from_url(product.get("url", ""))
        if pid:
            ids.append(pid)
    return ids

# ==============================
# Fetch pages of IDs
# ==============================

def fetch_page(url, headers, params, page, retries=3, backoff_factor=2):
    params = params.copy()
    params["page"] = page
    for attempt in range(retries):
        try:
            print(f"Fetching page {page + 1}...") # Added print statement for page fetching
            r = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=10)
            r.raise_for_status()
            return clean_and_extract_product_ids(r.json())
        except requests.exceptions.Timeout as e:
            print(f"Timeout fetching page {page + 1} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for page {page + 1}. Skipping.")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error fetching page {page + 1}: {e}. Status Code: {e.response.status_code}")
            if e.response.status_code == 429 and attempt < retries - 1:
                retry_after = int(e.response.headers.get("Retry-After", "5"))
                print(f"Rate limited. Retrying after {retry_after} seconds for page {page + 1}.")
                time.sleep(retry_after)
            elif e.response.status_code >= 500 and attempt < retries - 1:
                # Retry on server errors
                print(f"Server error {e.response.status_code}. Retrying page {page + 1}.")
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Skipping page {page + 1} due to unrecoverable HTTP error.")
                break
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error fetching page {page + 1} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for page {page + 1}. Skipping.")
        except Exception as e:
            print(f"Unexpected error fetching page {page + 1} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for page {page + 1}. Skipping.")
    return []

def fetch_product_ids(url_config, max_threads=5):
    base_headers ={
        "authority": "www.katespade.com",
        "method": "GET",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4",
        "referer": url_config["referer"],
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        #"x-sid": "0a7139ad-ccdb-48f1-beb1-291ca2bf5dbb",
        # Cookies: paste the full cookie string exactly as captured
       #"cookie": "akacd_kate-na-prod-new=2147483647~rv=76~id=7ae92db87f1322a97440e9fc6e047940; opt_user=5540f2f9-3a10-4836-b462-3cf7f9e5ffc3; ..."  # shortened for clarity
    }

    url = url_config["url"]
    params = url_config["params"]
    # First page to get totalPages
    print(f"Fetching initial page for {url}...")
    for attempt in range(3):  # Retry initial page up to 3 times
        try:
            r = requests.get(url, headers=base_headers, params=params, timeout=10)
            r.raise_for_status()
            #print(r.text)
            data = r.json()
            break  # Success, exit retry loop
        except requests.exceptions.Timeout as e:
            print(f"Timeout fetching initial page (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print("Max retries reached for initial page. Exiting.")
                return []
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error fetching initial page: {e}. Status Code: {e.response.status_code}")
            if e.response.status_code == 429 and attempt < 2:
                retry_after = int(e.response.headers.get("Retry-After", "5"))
                print(f"Rate limited. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
            elif e.response.status_code >= 500 and attempt < 2:
                print(f"Server error {e.response.status_code}. Retrying initial page.")
                time.sleep(2 ** attempt)
            else:
                print("Unrecoverable HTTP error for initial page. Exiting.")
                return []
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error fetching initial page (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print("Max retries reached for initial page. Exiting.")
                return []
        except Exception as e:
            print(f"Unexpected error fetching initial page (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print("Max retries reached for initial page. Exiting.")
                return []

    total_pages = data.get("pageData", {}).get("totalPages", 1)
    ids = clean_and_extract_product_ids(data)
    print(f"Total pages: {total_pages}")
    # Concurrently fetch rest
    with ThreadPoolExecutor(max_workers=max_threads) as ex:
        futures = {
            ex.submit(fetch_page, url, base_headers, params, p): p
            for p in range(1, total_pages)
        }
        for f in as_completed(futures):
            try:
                result_ids = f.result()
                if result_ids:
                    ids.extend(result_ids)
            except Exception as e:
                print(f"Exception in page fetch: {e}")

    return list(set(ids))

# ==============================
# Helper to fetch a single batch of product details
# ==============================
def _fetch_product_details_batch(batch, batch_index, base_url, headers, proxies, retries, backoff_factor):
    params = {
        "ids": ",".join(batch),
        "includeInventory": "true"
    }

    for attempt in range(retries):
        try:
            print(f"Fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries})... IDs: {batch[0]}...{batch[-1]}")
            r = requests.get(base_url, headers=headers, params=params, proxies=proxies, timeout=15)
            #print(r.text)
            r.raise_for_status()
            return r.json().get("productsData", [])
        except requests.exceptions.Timeout as e:
            print(f"Timeout fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for batch {batch_index}. Returning empty data for this batch.")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error fetching product details for batch {batch_index}: {e}. Status Code: {e.response.status_code}")
            if e.response.status_code == 429 and attempt < retries - 1:
                retry_after = int(e.response.headers.get("Retry-After", "5"))
                print(f"Rate limited. Retrying after {retry_after} seconds for batch {batch_index}.")
                time.sleep(retry_after)
            elif e.response.status_code >= 500 and attempt < retries - 1:
                # Retry on server errors
                print(f"Server error {e.response.status_code}. Retrying batch {batch_index}.")
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Skipping batch {batch_index} due to unrecoverable HTTP error.")
                break
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for batch {batch_index}. Returning empty data for this batch.")
        except Exception as e:
            print(f"Unexpected error fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for batch {batch_index}. Returning empty data for this batch.")
    return [] # Return empty list if all retries fail or unrecoverable error

# ==============================
# Fetch product details by IDs
# ==============================

def fetch_product_details(ids_list, batch_size=20, max_batch_threads=5, retries=3, backoff_factor=2):
    """Call /api/get-products with ids in concurrent batches."""
    all_product_data = []
    base_url = "https://www.katespadeoutlet.com/api/get-products"
    headers = {
        "authority": "www.katespadeoutlet.com",
        "method": "GET",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",
        "referer": "https://www.katespadeoutlet.com",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        # This is important, extracted from your browser request:
        #"cookie": "opt_user=b1fd4a23-6a01-4352-ada0-65f89ff84cd0; akacd_coach-na-prd-new-us=2147483647~rv=10~id=adf3dc842c599c46477cfef4a2557533; optimizelyEndUserId=oeu1752042994485r0.10466429737259508; FPID=FPID2.2.1xpNMqA2atpKzjNM%2FQty1tCwMc6Swe9zZ13odofRXa0%3D.1752042996; ..."  # (paste full cookie string from your browser)
    }
    batches_to_process = []
    for i in range(0, len(ids_list), batch_size):
        batches_to_process.append((ids_list[i:i + batch_size], i // batch_size + 1))

    with ThreadPoolExecutor(max_workers=max_batch_threads) as executor:
        futures = {
            executor.submit(
                _fetch_product_details_batch,
                batch,
                batch_index,
                base_url,
                headers,
                proxies,
                retries,
                backoff_factor
            ): (batch, batch_index)
            for batch, batch_index in batches_to_process
        }

        for future in as_completed(futures):
            batch, batch_index = futures[future]
            try:
                result_data = future.result()
                if result_data:
                    all_product_data.extend(result_data)
            except Exception as e:
                print(f"Exception processing batch {batch_index}: {e}")

    return {"productsData": all_product_data}

# ==============================
# Clean product data
# ==============================

def clean_katespade_data(data, gender_tag=None):
    """
    Cleans and processes raw product data from Kate Spade (or similar Shopify-like structure)
    into a standardized format suitable for e-commerce platforms.

    Args:
        data (dict): The raw product data, expected to contain a "productsData" key.
        gender_tag (str, optional): A string indicating the gender ("men" or "women")
                                    to apply as a tag. Defaults to None.

    Returns:
        list: A list of dictionaries, where each dictionary represents a cleaned product
              with its variants.
    """
    cleaned_products = {}

    # Iterate through each product entry in the raw data
    for product in data.get("productsData", []):
        # We only process 'master' or 'variation_group' hit types as they contain the core product info.
        # 'variation_group' often acts as the primary product entry for items with color variations but no size.
        if product.get("hitType") not in ["master", "variation_group"]:
            continue

        # Extract core product information
        handle = product.get("id")
        title = product.get("name")
        description = product.get("longDescription", "")
        vendor = product.get("brand")  # Use brand from the product data

        # Process raw category to determine product type
        raw_category = product.get("item_category", [""])[-1] if product.get("item_category") else ""
        product_type = raw_category.split("&", 1)[0].strip() # Split on '&' and take the first part

        product_category = product.get("custom", {}).get("c_classification", "")
        
        # Initialize a set to store unique tags
        tags_set = set()

        # Collect tags from all variants' custom attributes if available
        if product.get("variant"):
            for variant_data in product.get("variant"):
                ca = variant_data.get("customAttributes", {})
                if ca.get("c_aIMetaDataAttributes"):
                    tags_set.update(ca["c_aIMetaDataAttributes"].split(','))
                if ca.get("c_aIMetaDataSynonyms"):
                    tags_set.update(ca["c_aIMetaDataSynonyms"].split(','))
        
        # Also collect tags from the master product's custom attributes if available
        master_custom_attributes = product.get("master", {}).get("customAttributes", {})
        if master_custom_attributes.get("c_aIMetaDataAttributes"):
            tags_set.update(master_custom_attributes["c_aIMetaDataAttributes"].split(','))
        if master_custom_attributes.get("c_aIMetaDataSynonyms"):
            tags_set.update(master_custom_attributes["c_aIMetaDataSynonyms"].split(','))

        # Add gender-specific tags if provided and applicable
        gender_tags = set()
        if gender_tag:
            # Check if the product is clothing based on breadcrumbs
            is_clothing = any("clothing" in bc.get("htmlValue", "").lower() for bc in product.get("breadcrumbs", []))
            if gender_tag.lower() == "men":
                gender_tags.update({"men", "men's"})
                if is_clothing:
                    gender_tags.update({"all clothing men", "mens", "men clothing"})
            elif gender_tag.lower() == "women":
                gender_tags.update({"women", "women's"})
                if is_clothing:
                    gender_tags.update({"all clothing women", "womens", "women clothing"})
        tags_set.update(gender_tags)

        # Add product type and category as tags (including split by '&' for parts)
        extra_tags = []
        for val in [product_type, product_category]:
            if val:
                extra_tags.append(val)
                if "&" in val:
                    parts = [p.strip() for p in val.split("&") if p.strip()]
                    extra_tags.extend(parts)
        tags_set.update(extra_tags)

        # Build the final comma-separated tags string
        tags = ', '.join(sorted([t.strip() for t in tags_set if t.strip()]))
        
        # Special handling for "dress" in description/title/tags
        text_to_check = f"{description} {title} {tags}".lower()
        if "dress" in text_to_check:
            product_type = "Dress"

        # Collect all images, excluding .mp4 videos
        all_images = []
        for group in product.get("imageGroups", []):
            for img in group.get("images", []):
                src = img.get("src", "")
                if src and not src.lower().endswith(".mp4"):
                    all_images.append(src)
        # all_images = list(set(all_images))  # Removed - order preserved during collection # Remove duplicates

        # Initialize product entry in cleaned_products if not already present
        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": vendor,
                "Product Category": product_category,
                "Type": product_type,
                "Tags": tags,
                "variants": []
            }

        seen_variants = set()
        variants_to_process = product.get("variant", [])

        # If no explicit variants are listed, try to create one from default info
        # if it implies a single saleable item (e.g., a default variant or variant group)
        if not variants_to_process:
            default_variant_data = None
            if product.get("defaultVariant"):
                default_variant_data = product["defaultVariant"]
            elif product.get("defaultVariantGroup"):
                # If defaultVariantGroup has pricing and is orderable, treat it as the main variant
                default_vg = product["defaultVariantGroup"]
                if default_vg.get("orderable") and default_vg.get("pricingInfo"):
                    temp_variant = {
                        "id": default_vg.get("id"),
                        "offers": default_vg.get("offers", {}),
                        "pricingInfo": default_vg.get("pricingInfo", []),
                        "variationValues": {
                            "color": default_vg.get("customAttributes", {}).get("c_color")
                        }
                    }
                    default_variant_data = temp_variant

            if default_variant_data:
                variants_to_process.append(default_variant_data)

        # Process each variant found or constructed
        for variant in variants_to_process:
            avail = variant.get("offers", {}).get("availability", "")
            # Continue only if in stock, limited availability, or in-store only
            if "InStock" not in avail and "LimitedAvailability" not in avail and "InStoreOnly" not in avail:
                continue

            sku = variant.get("id", "")
            
            # Safely access pricing information
            pricing_info = variant.get("pricingInfo")
            sale_price = 0
            list_price = 0

            # Filter pricing entries to ensure they are dictionaries and not None
            valid_pricing = [
                entry for entry in (pricing_info or [])
                if isinstance(entry, dict) and entry is not None
            ]

            if valid_pricing:
                first_pricing_entry = valid_pricing[0]
                # Defensive check: ensure first_pricing_entry is not None before accessing its methods
                if first_pricing_entry is not None:
                    # Safely get sales price
                    sales_data = first_pricing_entry.get("sales", {})
                    sale_price = sales_data.get("value", 0) if isinstance(sales_data, dict) else 0
                    
                    # Safely get list price
                    list_data = first_pricing_entry.get("list", {})
                    list_price = list_data.get("value", 0) if isinstance(list_data, dict) else 0
                else:
                    # Fallback if first_pricing_entry somehow becomes None despite filtering
                    sale_price = 0
                    list_price = 0
            
            # Skip variant if both prices are 0 (indicates no valid pricing)
            if not sale_price and not list_price:
                continue

            # Extract variant attributes
            size = variant.get("variationValues", {}).get("size", "OS") # Default to "OS" (One Size) if no size is specified
            color_id = variant.get("variationValues", {}).get("color", "")
            color_name = ""
            
            # Try to find color name from product's color list
            for c in product.get("colors", []):
                if c.get("id") == color_id:
                    color_name = c.get("text", "")
                    break
            
            # If color_name is still empty, try to get it from the variant's custom attributes
            if not color_name:
                color_name = variant.get("customAttributes", {}).get("c_colorVal", "")

            # Create a unique key for the variant to prevent duplicates
            vkey = (sku, size, color_name)
            if vkey not in seen_variants:
                variant_images = []
                # Prioritize color-specific images
                for c in product.get("colors", []):
                    if c.get("id") == color_id:
                        for media_item in c.get("media", {}).get("full", []):
                            src = media_item.get("src", "")
                            if src and not src.lower().endswith(".mp4"):
                                variant_images.append(src)
                        break
                # Fallback to all_images if no color-specific images found for this color
                if not variant_images:
                    variant_images = all_images

                # Append the cleaned variant to the product's variants list
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color_name,
                    "Variant Price": float(sale_price),
                    "Variant Compare At Price": float(list_price),
                    "images": variant_images # Ensure unique images for the variant
                })
                seen_variants.add(vkey) # Add to seen set

    return list(cleaned_products.values()) # Return a list of all cleaned products


# ==============================
# Main
# ==============================


def complete_workflow_kate():
    url_configs = [
        {
            "url": "https://www.katespade.com/api/get-shop/sale/view-all",
            "params": {
                       "srule": "price-low-to-high"
            },
        "referer": "https://www.katespade.com/shop/sale/view-all?srule=price-low-to-high",
        }
    ]

    final_data = []
    for config in url_configs:
        gender = "women" if "women" in config["url"] else "women"
        ids = fetch_product_ids(config, max_threads=2) # max_threads for page fetching
        print(f"Collected {len(ids)} IDs for {gender}")
        # Send all IDs in batches with threading
        details = fetch_product_details(ids, max_batch_threads=2) # max_batch_threads for details fetcing
        if details: # Only proceed if details were successfully fetched
            cleaned = clean_katespade_data(details, gender_tag=gender)
            final_data.extend(cleaned)
        time.sleep(1)
    upsert_all_product_data(final_data, BASE_URL, "USD")
    with open("kate_outlet_cleaned.json", "w", encoding="utf-8") as f:
       json.dump(final_data, f, indent=2, ensure_ascii=False)

    #print(f"✅ Saved {len(final_data)} cleaned products to coachoutlet_cleaned.json")   



if __name__ == "__main__":
    complete_workflow_kate()