import json
import os
import os
import re
import sys
import time

from dotenv import load_dotenv
from seleniumbase import Driver
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from db import upsert_all_product_data

# BSS_PL.publicAccessToken = "d2e6ee62da9d5158adadada8c59c4bb1";
BASE_URL = "https://www.katespade.com"
load_dotenv()

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
# Browser fetcher for JSON pages (SeleniumBase)
# ==============================

def fetch_json_from_pre(driver, url: str):
    """Open a JSON endpoint in Chrome and extract JSON from <pre> using a shared SeleniumBase Driver"""
    driver.get(url)
    raw_json = driver.get_text("pre")
    return json.loads(raw_json)

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
    
    try:
        data = fetch_json_from_pre(driver, full_url)
        total_pages = data.get("pageData", {}).get("totalPages", 1)
        ids.extend(clean_and_extract_product_ids(data))
    except Exception as e:
        print(f"Error fetching first page: {e}")
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

    return list(set(ids))

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
    base_url = "https://www.katespadeoutlet.com/api/get-products"
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
    driver = Driver(uc=True, headless=True)
    try:
        for config in url_configs:
            gender = "women" if "women" in config["url"] else "women"
            ids = fetch_product_ids(driver, config)
            print(f"Collected {len(ids)} IDs for {gender}")
            
            # Send all IDs in batches
            details = fetch_product_details(driver, ids)
            if details: # Only proceed if details were successfully fetched
                cleaned = clean_katespade_data(details, gender_tag=gender)
                final_data.extend(cleaned)
            time.sleep(1)
            
        upsert_all_product_data(final_data, BASE_URL, "USD")
        with open("kate_outlet_cleaned.json", "w", encoding="utf-8") as f:
           json.dump(final_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Saved {len(final_data)} cleaned products to kate_outlet_cleaned.json")
    finally:
        driver.quit()   



if __name__ == "__main__":
    complete_workflow_kate()