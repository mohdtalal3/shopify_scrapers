from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
import asyncio

from dotenv import load_dotenv
import requests
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.http_clients import HttpxHttpClient

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
# Fetch product details using Crawlee + Playwright
# ==============================

async def fetch_product_details(ids_list, batch_size=50):
    """Fetch product details using Crawlee with Playwright for better bot detection handling."""
    from datetime import timedelta
    
    all_product_data = []
    
    # Create batches of IDs
    batches = []
    for i in range(0, len(ids_list), batch_size):
        batch_ids = ids_list[i:i + batch_size]
        batches.append(",".join(batch_ids))
    
    # Create URLs for each batch
    urls_to_crawl = []
    for batch_ids_str in batches:
        url = f"https://www.katespadeoutlet.com/api/get-products?ids={batch_ids_str}&includeInventory=true"
        urls_to_crawl.append(url)
    
    print(f"Created {len(urls_to_crawl)} batch URLs to crawl")
    
    # Configure the crawler
    crawler = PlaywrightCrawler(
        headless=True,
        browser_type='chromium',
        max_requests_per_crawl=len(urls_to_crawl) + 10,
        request_handler_timeout=timedelta(seconds=60),
        browser_options={
            'args': ['--no-sandbox', '--disable-setuid-sandbox']
        }
    )
    
    # Define the request handler
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}...')
        
        try:
            # Wait for the page to load
            await context.page.wait_for_load_state('domcontentloaded', timeout=30000)
            
            # Get the page content
            content = await context.page.content()
            
            # Try to extract JSON from the page
            try:
                # Check if it's a JSON response
                json_text = await context.page.locator('pre').inner_text()
                data = json.loads(json_text)
            except:
                # If not in <pre>, try to parse the entire body
                try:
                    json_text = await context.page.locator('body').inner_text()
                    data = json.loads(json_text)
                except:
                    context.log.warning(f'Could not parse JSON from {context.request.url}')
                    return
            
            # Extract product data
            products_data = data.get("productsData", [])
            if products_data:
                context.log.info(f'Found {len(products_data)} products in batch')
                all_product_data.extend(products_data)
            else:
                context.log.warning(f'No products found in response from {context.request.url}')
                
        except Exception as e:
            context.log.error(f'Error processing {context.request.url}: {e}')
    
    # Run the crawler
    await crawler.run(urls_to_crawl)
    
    print(f"✅ Crawled {len(all_product_data)} total products")
    
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

        # Collect images - try master imageGroups first, then fall back to variationGroup
        all_images = []
        seen_images = set()
        for group in product.get("imageGroups", []):
            for img in group.get("images", []):
                src = img.get("src", "")
                if src and not src.lower().endswith(".mp4") and src not in seen_images:
                    all_images.append(src)
                    seen_images.add(src)
        
        # If no images found at master level, try variationGroup
        if not all_images:
            for vg in product.get("variationGroup", []):
                for group in vg.get("imageGroups", []):
                    for img in group.get("images", []):
                        src = img.get("src", "")
                        if src and not src.lower().endswith(".mp4") and src not in seen_images:
                            all_images.append(src)
                            seen_images.add(src)
                # Once we find images in one variation group, use those
                if all_images:
                    break

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

async def complete_workflow_kate_outlet():
    print("Extracting URLs from sitemap...")
    urls = extract_urls_from_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} product URLs")

    ids = [extract_product_id_from_url(u) for u in urls if u]
    ids = [normalize_product_id(i) for i in ids if i]
    ids = list({i for i in ids if i})  # unique normalized IDs
    print(f"Extracted {len(ids)} normalized product IDs")
    # ids=ids[:10]  # Uncomment to test with fewer products
    
    details = await fetch_product_details(ids, batch_size=50)
    cleaned = clean_katespade_outlet_data(details)

    upsert_all_product_data(cleaned, BASE_URL, "USD")
    with open("kate_outlet_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(cleaned)} cleaned products to kate_outlet_cleaned.json")


if __name__ == "__main__":
    asyncio.run(complete_workflow_kate_outlet())
