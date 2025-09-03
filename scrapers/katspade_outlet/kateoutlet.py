from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

BASE_URL = "https://www.katespadeoutlet.com"
SITEMAP_URL = "https://www.katespadeoutlet.com/sitemap_0-product.xml"
load_dotenv()

proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None


# ==============================
# Utility functions
# ==============================

def normalize_product_id(pid: str) -> str:
    """Normalize product ID: uppercase, remove non-alphanumeric chars"""
    if not pid:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", pid).upper()


def extract_product_id_from_url(product_url: str) -> str:
    """Extract normalized product ID from product URL"""
    if not product_url:
        return ""
    try:
        last_part = product_url.split("/")[-1]
        base = last_part.split(".html")[0]
        pid = base.split("-")[0]
        return normalize_product_id(pid)
    except Exception:
        return ""


def extract_urls_from_sitemap(sitemap_url, proxies=None):
    """Extract only <loc> URLs from sitemap XML"""
    resp = requests.get(sitemap_url, proxies=proxies, timeout=15)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    return [loc.text for loc in root.findall(".//ns:url/ns:loc", ns)]



# ==============================
# Helper to fetch a single batch of product details
# ==============================

def _fetch_product_details_batch(batch, batch_index, base_url, headers, proxies, retries=3, backoff_factor=2):
    params = {
        "ids": ",".join(batch),
        "includeInventory": "true"
    }
    for attempt in range(retries):
        try:
            print(f"Fetching product details for batch {batch_index}, IDs: {batch[0]}...{batch[-1]}")
            r = requests.get(base_url, headers=headers, params=params, proxies=proxies, timeout=15)
            r.raise_for_status()
            return r.json().get("productsData", [])
        except Exception as e:
            print(f"Error fetching batch {batch_index} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                return []
    return []


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
    }

    batches = []
    for i in range(0, len(ids_list), batch_size):
        batches.append((ids_list[i:i + batch_size], i // batch_size + 1))

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
            for batch, batch_index in batches
        }

        for future in as_completed(futures):
            try:
                result_data = future.result()
                if result_data:
                    all_product_data.extend(result_data)
            except Exception as e:
                print(f"Exception processing batch: {e}")

    return {"productsData": all_product_data}


# ==============================
# Clean product data
# ==============================

def clean_katespade_outlet_data(data):
    cleaned_products = {}

    for product in data.get("productsData", []):
        if product.get("hitType") not in ["master", "variation_group"]:
            continue

        handle = product.get("id")
        title = product.get("name")
        description = product.get("longDescription", "")
        vendor = product.get("brand")

        raw_category = product.get("item_category", [""])[-1] if product.get("item_category") else ""
        product_type = raw_category.split("&", 1)[0].strip()
        product_category = product.get("custom", {}).get("c_classification", "")

        tags_set = set()

        # Collect metadata tags
        if product.get("variant"):
            for variant_data in product.get("variant"):
                ca = variant_data.get("customAttributes", {})
                if ca.get("c_aIMetaDataAttributes"):
                    tags_set.update(ca["c_aIMetaDataAttributes"].split(","))
                if ca.get("c_aIMetaDataSynonyms"):
                    tags_set.update(ca["c_aIMetaDataSynonyms"].split(","))

        master_custom_attributes = product.get("master", {}).get("customAttributes", {})
        if master_custom_attributes.get("c_aIMetaDataAttributes"):
            tags_set.update(master_custom_attributes["c_aIMetaDataAttributes"].split(","))
        if master_custom_attributes.get("c_aIMetaDataSynonyms"):
            tags_set.update(master_custom_attributes["c_aIMetaDataSynonyms"].split(","))

        # Always add women tags (outlet is women’s only)
        is_clothing = any("clothing" in bc.get("htmlValue", "").lower() for bc in product.get("breadcrumbs", []))
        gender_tags = {"women", "women's"}
        if is_clothing:
            gender_tags |= {"all clothing women", "womens", "women clothing"}
        tags_set.update(gender_tags)

        # Add product type and category
        for val in [product_type, product_category]:
            if val:
                tags_set.add(val)
                if "&" in val:
                    tags_set.update([p.strip() for p in val.split("&") if p.strip()])

        tags = ", ".join(sorted([t.strip() for t in tags_set if t.strip()]))

        # Collect images
        all_images = []
        for group in product.get("imageGroups", []):
            for img in group.get("images", []):
                src = img.get("src", "")
                if src and not src.lower().endswith(".mp4"):
                    all_images.append(src)

        cleaned_products[handle] = {
            "Handle": handle,
            "Title": title,
            "Body (HTML)": description,
            "Vendor": vendor,
            "Product Category": product_category,
            "Type": product_type,
            "Gender": "Women",
            "Tags": tags,
            "variants": []
        }

        seen = set()
        for variant in product.get("variant", []):
            avail = variant.get("offers", {}).get("availability", "")
            if "InStock" not in avail and "LimitedAvailability" not in avail and "InStoreOnly" not in avail:
                continue

            sku = variant.get("id", "")
            pricing_info = variant.get("pricingInfo") or []
            sale_price, list_price = 0, 0
            if pricing_info and isinstance(pricing_info, list) and pricing_info[0]:
                sales_data = pricing_info[0].get("sales", {})
                list_data = pricing_info[0].get("list", {})
                sale_price = sales_data.get("value", 0) if isinstance(sales_data, dict) else 0
                list_price = list_data.get("value", 0) if isinstance(list_data, dict) else 0

            if not sale_price and not list_price:
                continue

            size = variant.get("variationValues", {}).get("size", "OS")
            color_id = variant.get("variationValues", {}).get("color", "")
            color_name = ""
            for c in product.get("colors", []):
                if c.get("id") == color_id:
                    color_name = c.get("text", "")
                    break
            if not color_name:
                color_name = variant.get("customAttributes", {}).get("c_colorVal", "")

            vkey = (sku, size, color_name)
            if vkey not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color_name,
                    "Variant Price": float(sale_price),
                    "Variant Compare At Price": float(list_price),
                    "images": all_images
                })
                seen.add(vkey)

        # 🚨 Remove product if no variants
        if not cleaned_products[handle]["variants"]:
            del cleaned_products[handle]

    return list(cleaned_products.values())


# ==============================
# Main
# ==============================

def complete_workflow_kate_outlet():
    print("Extracting URLs from sitemap...")
    urls = extract_urls_from_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} product URLs")

    ids = [extract_product_id_from_url(u) for u in urls if u]
    ids = [normalize_product_id(i) for i in ids if i]
    ids = list({i for i in ids if i})  # unique normalized IDs
    print(f"Extracted {len(ids)} normalized product IDs")
    ids=ids[:10]
    details = fetch_product_details(ids, max_batch_threads=1)
    cleaned = clean_katespade_outlet_data(details)

    upsert_all_product_data(cleaned, BASE_URL, "USD")
    with open("kate_outlet_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(cleaned)} cleaned products to kate_outlet_cleaned.json")


if __name__ == "__main__":
    complete_workflow_kate_outlet()
