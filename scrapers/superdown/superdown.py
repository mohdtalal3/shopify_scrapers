import os
import json
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

load_dotenv()

BASE_URL = "https://www.superdown.com/shop/cat/clothing/3699fc?navsrc=left"
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None

# === HEADERS (from your browser dump) ===
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "referer": BASE_URL,
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}


# === PRODUCT CLEANER (your function, slightly adjusted) ===
def clean_and_save_product_from_html(html, gender_tag=None):
    soup = BeautifulSoup(html, "html.parser")

    product_data = None
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            parsed = json.loads(script.string)
            if parsed.get("@type") == "Product":
                product_data = parsed
                break
        except Exception:
            continue

    if not product_data:
        return None

    handle = product_data.get("sku")
    title = product_data.get("name")
    brand = product_data.get("brand", {}).get("name", "")

    # description
    desc_parts = []
    if product_data.get("description"):
        desc_parts.append(product_data["description"])
    details = soup.select(".pdp-details__list li")
    for li in details:
        desc_parts.append(li.get_text(strip=True))
    description = "<p>" + "</p><p>".join(desc_parts) + "</p>"

    # type from breadcrumbs
    type_val = ""
    crumb = soup.select(".crumbs__item .crumbs__text")
    if crumb:
        type_val = crumb[-1].get_text(strip=True)

    type_val=product_data.get("description",type_val)
    # tags
    product_tags = []
    gender_tags = set()
    if gender_tag:
        if gender_tag.lower() == "men":
            gender_tags = {"all clothing men", "mens", "men clothing", "men"}
        elif gender_tag.lower() == "women":
            gender_tags = {"all clothing women", "womens", "women clothing", "women"}
    all_tags = product_tags + list(gender_tags)
    if type_val:
        all_tags.extend(type_val.split())
    tags = ", ".join(tag.strip() for tag in all_tags if tag.strip())

    # images
    all_images = []
    seen_images = set()
    carousel = soup.find("div", {"class": "image-carousel"})
    if carousel and carousel.get("data-images"):
        try:
            imgs = json.loads(carousel["data-images"])
            for url in imgs:
                if url not in seen_images:
                    all_images.append(url)
                    seen_images.add(url)
        except Exception:
            pass
    if product_data.get("image") and product_data["image"] not in seen_images:
        all_images.append(product_data["image"])

    # color
    color = ""
    color_el = soup.select_one(".pdp__spec--color")
    if color_el:
        color = color_el.get_text(strip=True)

    # variants
    variants = []
    seen = set()
    for size_opt in soup.select(".size-options input.size-options__radio"):
        size = size_opt.get("data-size")
        available = not size_opt.has_attr("disabled")
        sku = f"{handle}-{size}"
        price = float(size_opt.get("data-price", 0))
        compare_price = float(size_opt.get("data-retailprice", 0))

        if available and (size, sku) not in seen:
            variants.append({
                "Variant SKU": sku,
                "size": size,
                "color": color,
                "Variant Price": price,
                "Variant Compare At Price": compare_price,
                "images": all_images
            })
            seen.add((size, sku))

    cleaned_product = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": description,
        "Vendor": brand,
        "Product Category": gender_tag.lower() if gender_tag else "",
        "Type": type_val,
        "Tags": tags,
        "variants": variants
    }

    return cleaned_product


# === SCRAPER ===
def fetch_listing_page(page_num):
    url = f"{BASE_URL}&pageNum={page_num}" if page_num > 1 else BASE_URL
    links = []
    try:
        r = requests.get(url, headers=headers, proxies=proxies, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for li in soup.select("li.gc a[href*='/product/']"):
            link = "https://www.superdown.com" + li.get("href").split("?")[0]
            links.append(link)
    except Exception as e:
        print(f"[!] Error fetching listing page {page_num}: {e}")
    return links


def get_total_pages():
    try:
        r = requests.get(BASE_URL, headers=headers, proxies=proxies, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        pages = [int(a["data-page-num"]) for a in soup.select("div.pagination a[data-page-num]")]
        return max(pages) if pages else 1
    except Exception as e:
        print(f"[!] Failed to fetch total pages: {e}")
        return 1


def fetch_product(link, gender_tag="women"):
    try:
        r = requests.get(link, headers=headers, proxies=proxies, timeout=20)
        r.raise_for_status()
        product = clean_and_save_product_from_html(r.text, gender_tag)
        if product:
            print(f"✅ Done extracting product: {link}")
        else:
            print(f"⚠️ No product data found: {link}")
        return product
    except Exception as e:
        print(f"❌ Failed to extract product: {link} - {e}")
        return None


def complete_workflow_superdown():
    total_pages = get_total_pages()
    print(f"[*] Found {total_pages} pages of products")

    # Step 1: collect product links
    all_links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_listing_page, page) for page in range(1, total_pages + 1)]
        for f in as_completed(futures):
            all_links.extend(f.result())
    all_links = sorted(set(all_links))
    print(f"[*] Collected {len(all_links)} unique product links")
    print(f"🔍 Starting to process {len(all_links)} products...")

    # Step 2: fetch product details
    all_products = []
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_product, link, "women"): link for link in all_links}
        for f in as_completed(futures):
            result = f.result()
            if result:
                all_products.append(result)
                variants_count = len(result.get('variants', []))
            else:
                variants_count = 0
            processed_count += 1
            print(f"📊 Progress: {processed_count}/{len(all_links)} products processed ({variants_count} variants extracted)")

    upsert_all_product_data(all_products,"https://www.superdown.com/","USD")
    # Step 3: save JSON
    with open("superdown_cleaned_products.json", "w", encoding="utf-8") as f:
        json.dump(all_products, f, indent=2)

    print(f"[✔] Saved {len(all_products)} products to superdown_cleaned_products.json")
    print(f"🎉 COMPLETE! Total products saved: {len(all_products)}")


if __name__ == "__main__":
    complete_workflow_superdown()
