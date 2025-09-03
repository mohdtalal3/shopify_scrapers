import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv
import sys
import xml.etree.ElementTree as ET

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

BASE_URL = "https://www.coacho1utlet.com"
SITEMAP_URL = "https://www.coachoutlet.com/sitemap_0-product.xml"
load_dotenv()

proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None
print("Using proxies:", proxies)


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


def extract_urls_from_sitemap(sitemap_url):
    """Extract all product URLs from sitemap XML"""
    resp = requests.get(sitemap_url, proxies=proxies, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [loc.text for loc in root.findall(".//ns:loc", ns)]


# ==============================
# Fetch product details
# ==============================

def _fetch_product_details_batch(batch, batch_index, base_url, headers, proxies, retries=6, backoff_factor=4):
    params = {
        "ids": ",".join(batch),
        "includeInventory": "true",
        "__v__": "1mwo_Eurah2CTgR1svi5y"
    }
    for attempt in range(retries):
        try:
            print(f"Fetching batch {batch_index}, IDs {batch[0]}...{batch[-1]}")
            r = requests.get(base_url, headers=headers, params=params,proxies=proxies,timeout=60)
            r.raise_for_status()
            return r.json().get("productsData", [])
        except Exception as e:
            print(f"Error fetching batch {batch_index} attempt {attempt+1}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                return []
    return []


def fetch_product_details(ids_list, batch_size=20, max_batch_threads=8):
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

    batches = []
    for i in range(0, len(ids_list), batch_size):
        batches.append((ids_list[i:i + batch_size], i // batch_size + 1))

    with ThreadPoolExecutor(max_workers=max_batch_threads) as executor:
        futures = {
            executor.submit(_fetch_product_details_batch, batch, batch_index, base_url, headers, proxies): (batch, batch_index)
            for batch, batch_index in batches
        }
        for future in as_completed(futures):
            try:
                result_data = future.result()
                if result_data:
                    all_product_data.extend(result_data)
            except Exception as e:
                print(f"Exception in batch fetch: {e}")

    return {"productsData": all_product_data}


# ==============================
# Clean product data
# ==============================

def clean_coachoutlet_data(data):
    cleaned_products = {}
    for product in data.get("productsData", []):
        if product.get("hitType") != "master":
            continue
        handle = product.get("id")
        title = product.get("name")
        description = product.get("longDescription", "")
        vendor = 'COACH'

        raw_category = product.get("item_category", [""])[-1] if product.get("item_category") else ""
        product_type = raw_category.split("&", 1)[0].strip()
        product_category = product.get("custom", {}).get("c_classification", "")

        # Gender direct from API
        gender_tag = product.get("custom", {}).get("c_gender", "Unisex")

        # Enriched gender tagging logic
        gender_tags = set()
        if gender_tag:
            is_clothing = any("clothing" in bc.get("htmlValue", "").lower() for bc in product.get("breadcrumbs", []))
            if gender_tag.lower() == "men":
                gender_tags = {"men", "men's"} | ({"all clothing men", "mens", "men clothing"} if is_clothing else set())
            elif gender_tag.lower() == "women":
                gender_tags = {"women", "women's"} | ({"all clothing women", "womens", "women clothing"} if is_clothing else set())
            else:
                gender_tags = {"unisex"}

        # Collect tags
        tags_set = set()
        tags_set |= gender_tags

        # Add product type and category
        if product_type:
            tags_set.add(product_type)
        if product_category:
            tags_set.add(product_category)

        # Add variant-level AI metadata attributes
        if product.get("variant"):
            for variant_data in product.get("variant"):
                ca = variant_data.get("customAttributes", {})
                if ca.get("c_aIMetaDataAttributes"):
                    tags_set.update(ca["c_aIMetaDataAttributes"].split(","))
                if ca.get("c_aIMetaDataSynonyms"):
                    tags_set.update(ca["c_aIMetaDataSynonyms"].split(","))

        # Collect images
        all_images = []
        for group in product.get("imageGroups", []):
            for img in group.get("images", []):
                src = img.get("src", "")
                if src and not src.lower().endswith(".mp4"):
                    all_images.append(src)
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
                "Gender": gender_tag,
                "Tags": ", ".join(sorted([t.strip() for t in tags_set if t.strip()])),
                "variants": []
            }

        seen = set()
        for variant in product.get("variant", []):
            avail = variant.get("offers", {}).get("availability", "")
            if "InStock" not in avail and "LimitedAvailability" not in avail:
                continue
            sku = variant.get("id", "")
            if handle and handle not in sku:
                continue

            pricing_info = variant.get("pricingInfo", [])
            sale_price, list_price = 0, 0
            if pricing_info and isinstance(pricing_info, list) and pricing_info[0]:
                sales_data = pricing_info[0].get("sales", {})
                list_data = pricing_info[0].get("list", {})
                sale_price = sales_data.get("value", 0) if isinstance(sales_data, dict) else 0
                list_price = list_data.get("value", 0) if isinstance(list_data, dict) else 0

            if not sale_price and not list_price:
                continue

            size = variant.get("variationValues", {}).get("size", "")
            color_id = variant.get("variationValues", {}).get("color", "")
            color_name = ""
            for c in product.get("colors", []):
                if c.get("id") == color_id:
                    raw_color_name = c.get("text", "")
                    # Handle colors like "Gold/Sand/Chalk" - take the last part after splitting by "/"
                    if "/" in raw_color_name:
                        color_name = raw_color_name.split("/")[-1].strip()
                    else:
                        color_name = raw_color_name
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
# Main workflow
# ==============================

def complete_workflow_coachoutlet():
    print("Extracting URLs from sitemap...")
    urls = extract_urls_from_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} product URLs")

    ids = [extract_product_id_from_url(u) for u in urls if u]
    ids = [i for i in ids if i]  # remove blanks
    ids = list(set(ids))
    print(f"Extracted {len(ids)} product IDs")
    #ids=ids[:10]
    details = fetch_product_details(ids, max_batch_threads=1)
    cleaned = clean_coachoutlet_data(details)

    upsert_all_product_data(cleaned, BASE_URL, "USD")
    with open("coachoutlet_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(cleaned)} cleaned products to coachoutlet_cleaned.json")


if __name__ == "__main__":
    complete_workflow_coachoutlet()
