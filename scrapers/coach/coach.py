import requests
import json
import time
import asyncio
import os
from dotenv import load_dotenv
import sys
import xml.etree.ElementTree as ET
from datetime import timedelta
from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

BASE_URL = "https://www.coachoutlet.com"
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
# Fetch product details using Crawlee + Playwright
# ==============================

async def fetch_product_details(ids_list, batch_size=50, batches_per_session=5):
    """Fetch product details using Crawlee with Playwright for better bot detection handling."""
    
    all_product_data = []
    
    # Create batches of IDs
    batches = []
    for i in range(0, len(ids_list), batch_size):
        batch_ids = ids_list[i:i + batch_size]
        batches.append(",".join(batch_ids))
    
    # Create URLs for each batch
    urls_to_crawl = []
    for batch_ids_str in batches:
        url = f"https://www.coachoutlet.com/api/get-products?ids={batch_ids_str}&includeInventory=true"
        urls_to_crawl.append(url)
    
    print(f"Created {len(urls_to_crawl)} batch URLs to crawl")
    
    # Configure proxy if available
    proxy_config = None
    if proxy_str:
        # Parse proxy URL (format: http://user:pass@host:port)
        proxy_config = {'server': proxy_str}
    
    # Process batches in groups (new session every N batches)
    for session_num in range(0, len(urls_to_crawl), batches_per_session):
        session_urls = urls_to_crawl[session_num:session_num + batches_per_session]
        print(f"\n🔄 Starting new session {session_num//batches_per_session + 1} with {len(session_urls)} batches")
        
        # Create a new crawler for each session (fresh browser context)
        crawler = PlaywrightCrawler(
            headless=True,
            browser_type='chromium',
            max_requests_per_crawl=len(session_urls) + 10,
            request_handler_timeout=timedelta(seconds=60)
        )
        
        # Define the request handler for this session
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
        
        # Run the crawler for this session
        await crawler.run(session_urls)
        print(f"✅ Session {session_num//batches_per_session + 1} completed, total products so far: {len(all_product_data)}")
    
    print(f"\n✅ All sessions completed! Crawled {len(all_product_data)} total products")
    
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

async def complete_workflow_coachoutlet():
    print("Extracting URLs from sitemap...")
    urls = extract_urls_from_sitemap(SITEMAP_URL)
    print(f"Found {len(urls)} product URLs")

    ids = [extract_product_id_from_url(u) for u in urls if u]
    ids = [i for i in ids if i]  # remove blanks
    ids = list(set(ids))
    print(f"Extracted {len(ids)} product IDs")
    # ids=ids[:10]  # Uncomment to test with fewer products
    
    details = await fetch_product_details(ids, batch_size=50)
    cleaned = clean_coachoutlet_data(details)

    upsert_all_product_data(cleaned, BASE_URL, "USD")
    with open("coachoutlet_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(cleaned)} cleaned products to coachoutlet_cleaned.json")


if __name__ == "__main__":
    asyncio.run(complete_workflow_coachoutlet())
