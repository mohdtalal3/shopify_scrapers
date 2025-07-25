# from seleniumbase import SB
# from bs4 import BeautifulSoup
# import re
# import requests
# import json
# import time
# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# from db import upsert_all_product_data
# BASE_URL = "https://banditrunning.com/"

# def extract_visible_product_ids_from_html(html):
#     """Extract product handles from Bandit Running HTML using BeautifulSoup"""
#     soup = BeautifulSoup(html, "html.parser")
    
#     # Find all links that contain "/products/" similar to url.py approach
#     product_links = soup.find_all('a', href=re.compile(r'/products/'))
    
#     handles = set()  # Use set to avoid duplicates
    
#     print(f"[DEBUG] Found {len(product_links)} product links on page")
    
#     for link in product_links:
#         href = link.get('href', '')
#         if '/products/' in href:
#             # Check if this product is sold out by looking for the sold out indicator
#             # Find the parent container that might contain the sold out pills
#             parent_container = link.find_parent()
#             while parent_container and parent_container.name != 'body':
#                 # Look for the sold out pills structure
#                 sold_out_pills = parent_container.find('ul', class_='c-item-card__pills')
#                 if sold_out_pills:
#                     sold_out_pill = sold_out_pills.find('li', class_='btn btn-pill', string=re.compile(r'Sold Out', re.IGNORECASE))
#                     if sold_out_pill:
#                         print(f"[DEBUG] Skipped sold out product: {href}")
#                         break
#                 parent_container = parent_container.find_parent()
#             else:
#                 # If we didn't find a sold out indicator, extract the handle
#                 match = re.search(r'/products/([^?&#]+)', href)
#                 if match:
#                     handle = match.group(1)
#                     handles.add(handle)
#                     print(f"[DEBUG] Added product: {handle}")
#                 else:
#                     print(f"[DEBUG] Skipped link (couldn't extract handle): {href}")
#         else:
#             print(f"[DEBUG] Skipped link (not product): {href}")

#     print(f"[DEBUG] Extracted {len(handles)} available product handles")
#     return list(handles)

# def scrape_product_ids_from_collections(collections):
#     """Scrape product handles from Bandit Running collections using selenium similar to url.py"""
#     all_ids = set()
#     with SB(uc=True, headless=True) as sb:
#         for entry in collections:
#             url = entry["url"] if isinstance(entry, dict) else entry
#             page_num = 1
#             try:
#                 print(f"[INFO] Loading collection: {url}")
#                 sb.open(url)
#                 sb.sleep(3)
                
#                 # Scroll and load all products similar to url.py approach
#                 last_height = sb.execute_script("return document.body.scrollHeight")
#                 products_count = 0
#                 no_new_products_count = 0
                
#                 while True:
#                     # Scroll down to bottom
#                     sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#                     sb.sleep(3)
                    
#                     # Get current HTML and extract handles
#                     html = sb.get_page_source()
#                     ids = extract_visible_product_ids_from_html(html)
                    
#                     current_products = len(ids)
#                     if current_products > products_count:
#                         print(f"[✓] Found {current_products} products (was {products_count})")
#                         products_count = current_products
#                         all_ids.update(ids)
#                         no_new_products_count = 0
#                     else:
#                         no_new_products_count += 1
#                         print(f"[INFO] No new products found. Attempt {no_new_products_count}/3")
                        
#                     # If no new products found for 3 consecutive attempts, stop
#                     if no_new_products_count >= 3:
#                         print(f"[✓] No new products loading. Finished scrolling for {url}")
#                         break
                        
#                     # Check if page height changed
#                     new_height = sb.execute_script("return document.body.scrollHeight")
#                     if new_height == last_height:
#                         print("[INFO] Page height unchanged, trying more scrolling...")
#                         sb.execute_script("window.scrollBy(0, 500);")
#                         sb.sleep(2)
                    
#                     last_height = new_height
                
#                 print(f"[✓] Collection {url}: {len(all_ids)} total unique product handles found")
                
#             except Exception as e:
#                 print(f"[!] Error loading {url}: {e}")
                
#     return list(all_ids)

# def scrape_product_descriptions_and_images(product_handles):
#     """Fetch product data using .js endpoints similar to extract_data.py approach"""
#     product_data = {}
    
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#         'Accept': 'application/json, text/javascript, */*; q=0.01',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Connection': 'keep-alive',
#         'Referer': 'https://banditrunning.com/',
#     }
    
#     for i, handle in enumerate(product_handles):
#         try:
#             # Construct the .js endpoint URL
#             url = f"https://banditrunning.com/products/{handle}.js"
            
#             print(f"[INFO] Fetching data for {handle} ({i+1}/{len(product_handles)})")
            
#             response = requests.get(url, headers=headers, timeout=30)
            
#             if response.status_code == 200:
#                 try:
#                     data = response.json()
                    
#                     # Extract description (use from API data since it's already HTML)
#                     description = data.get('description', f"<p>{data.get('title', 'No description available')}</p>")
                    
#                     # Extract all images from images array and media array
#                     all_images = []
                    
#                     # Get images from the images array
#                     images = data.get('images', [])
#                     for img_url in images:
#                         if img_url.startswith('//'):
#                             img_url = f"https:{img_url}"
#                         all_images.append(img_url)
                    
#                     # Also check media array for additional images
#                     media = data.get('media', [])
#                     for media_item in media:
#                         if media_item.get('media_type') == 'image':
#                             img_url = media_item.get('src')
#                             if img_url:
#                                 if img_url.startswith('//'):
#                                     img_url = f"https:{img_url}"
#                                 if img_url not in all_images:
#                                     all_images.append(img_url)
                    
#                     # Remove duplicates while preserving order
#                     unique_images = []
#                     for img in all_images:
#                         if img not in unique_images:
#                             unique_images.append(img)
                    
#                     product_data[handle] = {
#                         'description': description,
#                         'images': unique_images,
#                         'raw_data': data  # Store raw data for cleaning function
#                     }
                    
#                     print(f"[✓] Successfully fetched: {handle} - {len(unique_images)} images")
                    
#                 except json.JSONDecodeError as e:
#                     print(f"[!] JSON decode error for {handle}: {e}")
#                     product_data[handle] = {
#                         'description': f"<p>No description available</p>",
#                         'images': [],
#                         'raw_data': None
#                     }
#             else:
#                 print(f"[!] Failed to fetch {handle}. Status code: {response.status_code}")
#                 product_data[handle] = {
#                     'description': f"<p>No description available</p>",
#                     'images': [],
#                     'raw_data': None
#                 }
                        
#         except Exception as e:
#             print(f"[!] Error scraping data for {handle}: {e}")
#             product_data[handle] = {
#                 'description': f"<p>No description available</p>",
#                 'images': [],
#                 'raw_data': None
#             }
        
#         time.sleep(0.5)  # Be respectful to the server
    
#     return product_data

# def ngrams_from_words(words, n):
#     return [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]

# def build_title_ngrams(title):
#     words = title.strip().split()
#     last3 = words[-3:] if len(words) >= 3 else words
#     ngram_tags = set()
#     for n in range(1, min(3, len(last3))+1):
#         ngram_tags.update(ngrams_from_words(last3, n))
#     return ngram_tags

# def clean_and_save_product_data_only_available_with_all_images_from_data(data, scraped_data=None):
#     """Clean Bandit Running product data based on raw_data.json structure"""
#     cleaned_products = {}
    
#     # Use provided scraped data
#     if scraped_data is None:
#         scraped_data = {}

#     for handle, product_info in scraped_data.items():
#         raw_data = product_info.get('raw_data')
#         if raw_data is None:
#             continue
            
#         title = raw_data.get("title", "")
        
#         # Get description and images from scraped data
#         description = product_info.get('description', f"<p>{title}</p>")
#         all_images = product_info.get('images', [])
        
#         brand = "Bandit Running"  # Set vendor to Bandit Running
#         product_tags = set(raw_data.get("tags", []))

#         # Determine gender from tags or URL
#         gender_tags = set()
#         if any(tag for tag in product_tags if 'women' in tag.lower()):
#             gender_tags = {"all clothing women", "womens", "women clothing", "women"}
#         elif any(tag for tag in product_tags if 'men' in tag.lower()):
#             gender_tags = {"all clothing men", "mens", "men clothing", "men"}
#         else:
#             # Default to unisex
#             gender_tags = {"unisex", "clothing"}

#         # N-grams from last 3 words of title
#         ngram_tags = build_title_ngrams(title)

#         all_tags = product_tags | gender_tags | ngram_tags
#         tags_str = ', '.join(sorted(all_tags))

#         # Determine category from gender tags
#         if "women" in ' '.join(gender_tags).lower():
#             category_val = "women"
#         elif "men" in ' '.join(gender_tags).lower():
#             category_val = "men"
#         else:
#             category_val = "unisex"
        
#         # Get product type from raw data
#         type_val = raw_data.get("type", "")

#         if handle not in cleaned_products:
#             cleaned_products[handle] = {
#                 "Handle": handle,
#                 "Title": title,
#                 "Body (HTML)": description,
#                 "Vendor": brand,
#                 "Product Category": category_val,
#                 "Type": type_val,
#                 "Tags": tags_str,
#                 "variants": []
#             }

#         seen = set()
#         for variant in raw_data.get("variants", []):
#             if not variant.get("available", False):
#                 continue

#             variant_id = variant.get("id", "")
#             price = float(variant.get("price", 0)) / 100  # Bandit Running prices are in cents
#             compare_price = float(variant.get("compare_at_price", 0)) / 100 if variant.get("compare_at_price") else 0
            
#             # Extract color and size from options array
#             color, size = "", ""
#             options = variant.get("options", [])
#             if len(options) >= 1:
#                 size = options[0]  # First option is usually size
#             if len(options) >= 2:
#                 color = options[1]  # Second option is usually color
            
#             # Use SKU if available, otherwise use size
#             sku = variant.get("sku", "") or size or variant.get("title", "")

#             if (size, variant_id) not in seen:
#                 # Use all scraped images for every variant
#                 cleaned_products[handle]["variants"].append({
#                     "Variant SKU": sku,
#                     "size": size,
#                     "color": color,
#                     "Variant Price": price,
#                     "Variant Compare At Price": compare_price,
#                     "images": all_images  # All variants get the complete image set
#                 })
#                 seen.add((size, variant_id))

#     # Return as a list of product dicts
#     return list(cleaned_products.values())

# def complete_workflow_bandi():
#     print("🔍 Scraping product handles from Bandit Running...")
#     collections = [
#         {"url": "https://banditrunning.com/collections/mens?filter.p.tag=filter-type:bottoms&filter.p.tag=filter-type:compression&filter.p.tag=filter-type:crew+necks&filter.p.tag=filter-type:half-tights&filter.p.tag=filter-type:hoodies&filter.p.tag=filter-type:long+sleeves&filter.p.tag=filter-type:longsleeves&filter.p.tag=filter-type:pants&filter.p.tag=filter-type:run+tees&filter.p.tag=filter-type:shorts&filter.p.tag=filter-type:sweatpants&filter.p.tag=filter-type:tanks&filter.p.tag=filter-type:tees&filter.p.tag=filter-type:tops"},
#         {"url": "https://banditrunning.com/collections/women?filter.p.tag=filter-type:bottoms&filter.p.tag=filter-type:compression&filter.p.tag=filter-type:crew+necks&filter.p.tag=filter-type:crop+singlets&filter.p.tag=filter-type:hoodies&filter.p.tag=filter-type:leggings&filter.p.tag=filter-type:long+sleeves&filter.p.tag=filter-type:outerwear&filter.p.tag=filter-type:run+tees&filter.p.tag=filter-type:run+tights&filter.p.tag=filter-type:shorts&filter.p.tag=filter-type:sweatpants&filter.p.tag=filter-type:tanks&filter.p.tag=filter-type:tees&filter.p.tag=filter-type:tops"}
#     ]
#     scraped_handles = scrape_product_ids_from_collections(collections)
#     unique_handles = list(set(scraped_handles))
    

#     print("📄 Scraping product data from .js endpoints...")
#     scraped_data = scrape_product_descriptions_and_images(unique_handles)
#     print(f"📄 Scraped data for {len(scraped_data)} products")

#     print(f"🧹 Cleaning data...")
#     all_products = []
#     cleaned = clean_and_save_product_data_only_available_with_all_images_from_data(None, scraped_data)
#     all_products.extend(cleaned)

#     # Remove duplicate products by handle (keep first occurrence)
#     seen_handles = set()
#     unique_products = []
#     for prod in all_products:
#         if prod["Handle"] not in seen_handles:
#             unique_products.append(prod)
#             seen_handles.add(prod["Handle"])

#         # Output JSON instead of uploading to database
#         print("📄 Outputting JSON data...")
#         output_data = {"products": unique_products}
        
#         # Save to file
#         output_file = "test_output.json"
#         with open(output_file, 'w', encoding='utf-8') as f:
#             json.dump(output_data, f, indent=2, ensure_ascii=False)
        
#         # Upload directly to database - COMMENTED OUT FOR NOW
#         print("📤 Uploading to database...")
#         upsert_all_product_data(unique_products, BASE_URL, "USD")
#         print(f"✅ {len(unique_products)} products uploaded to database successfully!")
        

# # 🔧 Run Everything
# if __name__ == "__main__":
#     # Run on just 5 products for testing and output JSON
#     complete_workflow_bandi()