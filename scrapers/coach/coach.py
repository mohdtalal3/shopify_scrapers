import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import re
# BSS_PL.publicAccessToken = "d2e6ee62da9d5158adadada8c59c4bb1";
BASE_URL = "https://www.coachoutlet.com"
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
    base_headers = {
        "authority": "www.coachoutlet.com",
        "method": "GET",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "referer": url_config["referer"],
    }

    url = url_config["url"]
    params = url_config["params"]

    # First page to get totalPages
    print(f"Fetching initial page for {url}...")
    for attempt in range(3):  # Retry initial page up to 3 times
        try:
            r = requests.get(url, headers=base_headers, params=params, proxies=proxies, timeout=10)
            r.raise_for_status()
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

def fetch_product_details(ids_list, batch_size=20, max_batch_threads=8, retries=3, backoff_factor=2):
    """Call /api/get-products with ids in concurrent batches."""
    all_product_data = []
    base_url = "https://www.coachoutlet.com/api/get-products"
    headers = {
        "authority": "www.coachoutlet.com",
        "method": "GET",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",
        "referer": "https://www.coachoutlet.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
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

def clean_coachoutlet_data(data, gender_tag=None):
    cleaned_products = {}
    for product in data.get("productsData", []):
        if product.get("hitType") != "master":
            continue
        handle = product.get("id")
        title = product.get("name")
        description = product.get("longDescription", "")
        vendor = 'COACH'

        # Get last category string if available
        raw_category = product.get("item_category", [""])[-1] if product.get("item_category") else ""

        # Split on '&' and take the first part
        product_type = raw_category.split("&", 1)[0].strip()

        product_category = product.get("custom", {}).get("c_classification", "")

        tags_set = set()
        if product.get("variant"):
            for variant_data in product.get("variant"):
                ca = variant_data.get("customAttributes", {})
                if ca.get("c_aIMetaDataAttributes"):
                    tags_set.update(ca["c_aIMetaDataAttributes"].split(','))
                if ca.get("c_aIMetaDataSynonyms"):
                    tags_set.update(ca["c_aIMetaDataSynonyms"].split(','))

        gender_tags = set()
        if gender_tag:
            is_clothing = any("clothing" in bc.get("htmlValue", "").lower() for bc in product.get("breadcrumbs", []))
            if gender_tag.lower() == "men":
                gender_tags = {"men","men's"} | ({"all clothing men", "mens", "men clothing"} if is_clothing else set())
            elif gender_tag.lower() == "women":
                gender_tags = {"women","women's"} | ({"all clothing women", "womens", "women clothing"} if is_clothing else set())
            else:
                gender_tags = {"unisex"}
        # Merge gender-specific tags
        tags_set |= gender_tags

        # Add product type and category as tags (including split by &)
        extra_tags = []
        for val in [product_type, product_category]:
            if val:
                extra_tags.append(val)
                if "&" in val:
                    parts = [p.strip() for p in val.split("&") if p.strip()]
                    extra_tags.extend(parts)

        tags_set |= set(extra_tags)

        # Build final tags string
        tags = ', '.join(sorted([t.strip() for t in tags_set if t.strip()]))


        all_images = []
        for group in product.get("imageGroups", []):
            for img in group.get("images", []):
                src = img.get("src", "")
                if src and not src.lower().endswith(".mp4"):
                    all_images.append(src)
        # Remove duplicates while preserving order
        seen_images = set()
        all_images = [img for img in all_images if not (img in seen_images or seen_images.add(img))]

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

        seen = set()
        for variant in product.get("variant", []):
            avail = variant.get("offers", {}).get("availability", "")
            if "InStock" not in avail and "LimitedAvailability" not in avail:
                continue

            sku = variant.get("id", "")

            # Skip variant if SKU doesn't contain the handle
            if handle and handle not in sku:
                continue

            # Safely access pricing information
            pricing_info = variant.get("pricingInfo")
            sale_price = 0
            list_price = 0

            if pricing_info and isinstance(pricing_info, list) and pricing_info:
                first_pricing_entry = pricing_info[0]
                if first_pricing_entry: # Ensure the dictionary itself isn't None
                    # Safely get sales price
                    sales_data = first_pricing_entry.get("sales", {})
                    sale_price = sales_data.get("value", 0) if isinstance(sales_data, dict) else 0
                    
                    # Safely get list price
                    list_data = first_pricing_entry.get("list", {})
                    list_price = list_data.get("value", 0) if isinstance(list_data, dict) else 0
            
            # Skip variant if price is 0
            if not sale_price and not list_price:
                continue

            size = variant.get("variationValues", {}).get("size", "")
            color_id = variant.get("variationValues", {}).get("color", "")
            color_name = ""
            for c in product.get("colors", []):
                if c.get("id") == color_id:
                    color_name = c.get("text", "")
                    break

            vkey = (sku, size, color_name)
            if vkey not in seen:
                variant_images = []
                for c in product.get("colors", []):
                    if c.get("id") == color_id:
                        for media_item in c.get("media", {}).get("full", []):
                            src = media_item.get("src", "")
                            if src and not src.lower().endswith(".mp4"):
                                variant_images.append(src)
                        break
                if not variant_images:
                    variant_images = all_images

                # Remove duplicates while preserving order
                seen_variant_images = set()
                variant_images = [img for img in variant_images if not (img in seen_variant_images or seen_variant_images.add(img))]

                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color_name,
                    "Variant Price": float(sale_price or 0),
                    "Variant Compare At Price": float(list_price or 0),
                    "images": variant_images
                })
                seen.add(vkey)
    return list(cleaned_products.values())

# ==============================
# Main
# ==============================


def complete_workflow_coachoutlet():
    url_configs = [
        {
            "url": "https://www.coachoutlet.com/api/get-shop/women/view-all",
            "params": {
                "srule": "bestsellers",
                "index": "0",
                "filterByDiscount": "50% – 60%|60% – 65%|65% – 70%|70% +"
            },
            "referer": "https://www.coachoutlet.com/shop/women/view-all"
        },
        {
            "url": "https://www.coachoutlet.com/api/get-shop/men/view-all",
            "params": {
                "srule": "bestsellers",
                "index": "0",
                "filterByDiscount": "50% – 60%|60% – 65%|65% – 70%|70% +"
            },
            "referer": "https://www.coachoutlet.com/shop/men/view-all"
        }
    ]

    final_data = []
    for config in url_configs:
        gender = "women" if "women" in config["url"] else "men"
        ids = fetch_product_ids(config, max_threads=2) # max_threads for page fetching
        print(f"Collected {len(ids)} IDs for {gender}")

        # Send all IDs in batches with threading
        details = fetch_product_details(ids, max_batch_threads=2) # max_batch_threads for details fetching
        if details: # Only proceed if details were successfully fetched
            cleaned = clean_coachoutlet_data(details, gender_tag=gender)
            final_data.extend(cleaned)
        time.sleep(1)
    upsert_all_product_data(final_data, BASE_URL, "USD")
    with open("coachoutlet_cleaned.json", "w", encoding="utf-8") as f:
       json.dump(final_data, f, indent=2, ensure_ascii=False)

    #print(f"✅ Saved {len(final_data)} cleaned products to coachoutlet_cleaned.json")   



if __name__ == "__main__":
    complete_workflow_coachoutlet()