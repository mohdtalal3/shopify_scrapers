import json
import time
import os
from dotenv import load_dotenv
import sys
import os
from seleniumbase import Driver
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data
import re
# BSS_PL.publicAccessToken = "d2e6ee62da9d5158adadada8c59c4bb1";
BASE_URL = "https://www.coachoutlet.com"
load_dotenv()

# ==============================
# Browser fetcher for JSON pages (SeleniumBase)
# ==============================

def fetch_json_from_pre(driver, url: str):
    """Open a JSON endpoint in Chrome and extract JSON from <pre> using a shared SeleniumBase Driver"""
    driver.get(url)
    raw_json = driver.get_text("pre")
    driver.save_screenshot("talal.png")
    return json.loads(raw_json)
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

def fetch_page(driver, url, params, page, retries=3, backoff_factor=2):
    from urllib.parse import urlencode, quote
    params = params.copy()
    params["page"] = page
    
    query_str = urlencode(params, quote_via=quote, safe="")
    full_url = f"{url}?{query_str}"
    
    for attempt in range(retries):
        try:
            print(f"Fetching page {page + 1}...")
            data = fetch_json_from_pre(driver, full_url)
            return clean_and_extract_product_ids(data)
        except Exception as e:
            print(f"Error fetching page {page + 1} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for page {page + 1}. Skipping.")
    return []

def fetch_product_ids(driver, url_config, max_retries=2):
    from urllib.parse import urlencode, quote
    
    url = url_config["url"]
    params = url_config["params"]
    
    # First page to get totalPages
    query_str = urlencode(params, quote_via=quote, safe="")
    full_url = f"{url}?{query_str}"
    
    print(f"Fetching initial page for {url}...")
    ids = []
    failed_pages = []
    
    # Try to fetch initial page with retry and proxy rotation
    data = None
    total_pages = 1
    
    for initial_attempt in range(3):  # Try initial page up to 3 times
        try:
            data = fetch_json_from_pre(driver, full_url)
            total_pages = data.get("pageData", {}).get("totalPages", 1)
            ids.extend(clean_and_extract_product_ids(data))
            print(f"Successfully fetched initial page on attempt {initial_attempt + 1}")
            break
        except Exception as e:
            print(f"Error fetching initial page (attempt {initial_attempt + 1}/3): {e}")
            if initial_attempt < 2:  # Don't recreate driver on last attempt
                print("Proxy may not be working, creating new driver with rotated proxy...")
                # Return current driver to caller so they can quit it and create a new one
                return "RETRY_WITH_NEW_DRIVER", None
            else:
                print("Max retries reached for initial page, continuing with limited data")
                failed_pages.append(0)
                total_pages = 1

    print(f"Total pages: {total_pages}")
    
    # Loop through all pages
    for page in range(1, total_pages):
        try:
            page_ids = fetch_page(driver, url, params, page)
            if page_ids:
                ids.extend(page_ids)
        except Exception as e:
            print(f"Error fetching page {page+1}: {e}")
            failed_pages.append(page)

    # Retry failed pages
    for attempt in range(max_retries):
        if not failed_pages:
            break
        print(f"Retrying failed pages: {failed_pages} (attempt {attempt+1})")
        still_failed = []
        for page in failed_pages:
            try:
                if page == 0:
                    data = fetch_json_from_pre(driver, full_url)
                    ids.extend(clean_and_extract_product_ids(data))
                else:
                    page_ids = fetch_page(driver, url, params, page)
                    if page_ids:
                        ids.extend(page_ids)
            except Exception as e:
                print(f"Retry failed for page {page+1}: {e}")
                still_failed.append(page)
        failed_pages = still_failed

    if failed_pages:
        print(f"Failed to fetch pages after retries: {failed_pages}")

    return "SUCCESS", list(set(ids))

# ==============================
# Helper to fetch a single batch of product details
# ==============================
def _fetch_product_details_batch(driver, batch, batch_index, base_url, retries=3, backoff_factor=2):
    from urllib.parse import urlencode, quote
    
    params = {
        "ids": ",".join(batch),
        "includeInventory": "true"
    }

    query_str = urlencode(params, quote_via=quote, safe=",")
    full_url = f"{base_url}?{query_str}"

    for attempt in range(retries):
        try:
            print(f"Fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries})... IDs: {batch[0]}...{batch[-1]}")
            data = fetch_json_from_pre(driver, full_url)
            return data.get("productsData", [])
        except Exception as e:
            print(f"Error fetching product details for batch {batch_index} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                print(f"Max retries reached for batch {batch_index}. Returning empty data for this batch.")
    return []

# ==============================
# Fetch product details by IDs
# ==============================

def fetch_product_details(driver, ids_list, batch_size=20, max_retries=2):
    """Call /api/get-products with ids in batches using SeleniumBase."""
    all_product_data = []
    base_url = "https://www.coachoutlet.com/api/get-products"
    failed_batches = []

    for i in range(0, len(ids_list), batch_size):
        batch = ids_list[i:i + batch_size]
        batch_index = i // batch_size + 1
        
        try:
            result_data = _fetch_product_details_batch(driver, batch, batch_index, base_url)
            if result_data:
                all_product_data.extend(result_data)
        except Exception as e:
            print(f"Error fetching batch {batch_index}: {e}")
            failed_batches.append((i, batch))
        time.sleep(1)

    # Retry failed batches
    for attempt in range(max_retries):
        if not failed_batches:
            break
        print(f"Retrying failed batches: {[i//batch_size+1 for i, _ in failed_batches]} (attempt {attempt+1})")
        still_failed = []
        for i, batch in failed_batches:
            batch_index = i // batch_size + 1
            try:
                result_data = _fetch_product_details_batch(driver, batch, batch_index, base_url)
                if result_data:
                    all_product_data.extend(result_data)
            except Exception as e:
                print(f"Retry failed for batch {batch_index}: {e}")
                still_failed.append((i, batch))
            time.sleep(1)
        failed_batches = still_failed

    if failed_batches:
        print(f"Failed to fetch batches after retries: {[i//batch_size+1 for i, _ in failed_batches]}")

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
    driver = Driver(uc=True, headless=True, proxy="abcc369ade8d7b3721e8:bd78aebb5825f219@gw.dataimpulse.com:823")
    
    try:
        for config in url_configs:
            gender = "women" if "women" in config["url"] else "men"
            
            # Try to fetch product IDs with retry and proxy rotation
            max_driver_retries = 3
            ids = []
            
            for driver_attempt in range(max_driver_retries):
                try:
                    result = fetch_product_ids(driver, config)
                    
                    if isinstance(result, tuple) and result[0] == "RETRY_WITH_NEW_DRIVER":
                        print(f"Driver retry requested (attempt {driver_attempt + 1}/{max_driver_retries})")
                        if driver_attempt < max_driver_retries - 1:
                            # Quit current driver and create new one with rotated proxy
                            driver.quit()
                            time.sleep(2)  # Brief pause before creating new driver
                            driver = Driver(uc=True, headless=True, proxy="abcc369ade8d7b3721e8:bd78aebb5825f219@gw.dataimpulse.com:823")
                            continue
                        else:
                            print("Max driver retries reached, proceeding with empty IDs")
                            ids = []
                            break
                    elif isinstance(result, tuple) and result[0] == "SUCCESS":
                        ids = result[1]
                        print(f"Collected {len(ids)} IDs for {gender}")
                        break
                    else:
                        # Handle legacy return format (just in case)
                        ids = result if isinstance(result, list) else []
                        print(f"Collected {len(ids)} IDs for {gender}")
                        break
                        
                except Exception as e:
                    print(f"Unexpected error in driver attempt {driver_attempt + 1}: {e}")
                    if driver_attempt < max_driver_retries - 1:
                        try:
                            driver.quit()
                        except:
                            pass
                        time.sleep(2)
                        driver = Driver(uc=True, headless=True, proxy="abcc369ade8d7b3721e8:bd78aebb5825f219@gw.dataimpulse.com:823")
                    else:
                        ids = []
                        break

            # Only proceed if we have IDs
            if ids:
                # Send all IDs in batches
                details = fetch_product_details(driver, ids)
                if details: # Only proceed if details were successfully fetched
                    cleaned = clean_coachoutlet_data(details, gender_tag=gender)
                    final_data.extend(cleaned)
            
            time.sleep(1)
            
        upsert_all_product_data(final_data, BASE_URL, "USD")
        with open("coachoutlet_cleaned.json", "w", encoding="utf-8") as f:
           json.dump(final_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Saved {len(final_data)} cleaned products to coachoutlet_cleaned.json")
    finally:
        try:
            driver.quit()
        except:
            pass   


if __name__ == "__main__":
    complete_workflow_coachoutlet()