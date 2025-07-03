import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import re
from urllib.parse import urljoin, urlparse
from db import upsert_product

# Constants
BASE_URL = "https://notorious-plug.com"
COLLECTIONS = {
    "womens": "/collections/womens",
    "clothing": "/collections/clothing",
    "shoes": "/collections/shoes"
}

# Default headers for web requests (using the working headers from extractor.py)
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Create a session object like in extractor.py
session = requests.Session()
session.headers.update(DEFAULT_HEADERS)

# ===== WEB SCRAPING MODULE (Using proven working logic from extractor.py) =====
def get_page_content(url, max_retries=3):
    """Fetch page content with error handling and retry logic - EXACT COPY from extractor.py"""
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return None

def get_total_pages(collection_url):
    """Extract total number of pages from pagination - EXACT COPY from extractor.py"""
    print(f"Determining total pages for: {collection_url}")
    
    content = get_page_content(collection_url)
    if not content:
        return 1
        
    soup = BeautifulSoup(content, 'html.parser')
    
    # Look for pagination elements
    pagination = soup.find('div', class_='Pagination')
    if not pagination:
        print("No pagination found, assuming single page")
        return 1
    
    # Find all pagination nav items
    nav_items = pagination.find_all(['a', 'span'], class_='Pagination__NavItem')
    
    # Look for the last page number before the "next" link
    last_page_number = 1
    
    for i, item in enumerate(nav_items):
        # Check if this is a "next" link
        if item.get('rel') == 'next' or (item.name == 'a' and 'next' in item.get('title', '').lower()):
            # The previous item should be the last page number
            if i > 0:
                prev_item = nav_items[i - 1]
                if prev_item.name == 'a':
                    href = prev_item.get('href', '')
                    # Extract page number from href
                    match = re.search(r'page=(\d+)', href)
                    if match:
                        last_page_number = int(match.group(1))
                        break
                elif prev_item.name == 'span':
                    # If it's a span, get the text content
                    text = prev_item.get_text(strip=True)
                    if text.isdigit():
                        last_page_number = int(text)
                        break
    
    # If we didn't find it through the next link method, try alternative approach
    if last_page_number == 1:
        # Look for all page numbers and take the highest one
        page_numbers = []
        for item in nav_items:
            if item.name == 'a':
                href = item.get('href', '')
                if 'page=' in href:
                    match = re.search(r'page=(\d+)', href)
                    if match:
                        page_num = int(match.group(1))
                        if page_num > 0 and page_num <= 1000:  # Sanity check
                            page_numbers.append(page_num)
            elif item.name == 'span':
                text = item.get_text(strip=True)
                if text.isdigit() and text != '…':
                    page_num = int(text)
                    if page_num > 0 and page_num <= 1000:  # Sanity check
                        page_numbers.append(page_num)
        
        if page_numbers:
            last_page_number = max(page_numbers)
    
    print(f"Found {last_page_number} total pages")
    return last_page_number

def get_collection_tags(collection_name):
    """Get tags for a specific collection - EXACT COPY from extractor.py"""
    tag_mapping = {
        'womens': ['Women', 'All Women'],
        'clothing': ['Men', 'All Men'],
        'shoes': ['Men', 'All Men', 'Mens Shoes']
    }
    return tag_mapping.get(collection_name, [])

def extract_product_links_from_page(page_content, base_url, collection_tags=None):
    """Extract product links and information from a page - EXACT COPY from extractor.py"""
    soup = BeautifulSoup(page_content, 'html.parser')
    products = []
    
    # Find all product items
    product_items = soup.find_all('div', class_='ProductItem__Info')
    
    for item in product_items:
        try:
            # Extract product title and link
            title_element = item.find('h2', class_='ProductItem__Title')
            if title_element:
                link_element = title_element.find('a')
                if link_element:
                    product_url = link_element.get('href', '')
                    product_title = link_element.get_text(strip=True)
                    
                    # Make URL absolute
                    if product_url.startswith('/'):
                        product_url = urljoin(base_url, product_url)
                    
                    # Extract pricing information
                    price_list = item.find('div', class_='ProductItem__PriceList')
                    current_price = ""
                    compare_price = ""
                    
                    if price_list:
                        # Current price (highlighted)
                        current_price_elem = price_list.find('span', class_='ProductItem__Price Price--highlight')
                        if current_price_elem:
                            current_price = current_price_elem.get_text(strip=True)
                        
                        # Compare price (original price)
                        compare_price_elem = price_list.find('span', class_='ProductItem__Price Price--compareAt')
                        if compare_price_elem:
                            compare_price = compare_price_elem.get_text(strip=True)
                    
                    product_data = {
                        'title': product_title,
                        'url': product_url,
                        'current_price': current_price,
                        'compare_price': compare_price,
                        'tags': collection_tags or [],  # Add tags to product data
                        'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    products.append(product_data)
                    print(f"  ✓ Extracted: {product_title}")
                    
        except Exception as e:
            print(f"Error extracting product: {e}")
            continue
    
    return products

def scrape_collection_urls(collection_path):
    """Scrape all pages of a collection to get product URLs - Using extractor.py logic"""
    collection_url = urljoin(BASE_URL, collection_path)
    collection_name = collection_path.split('/')[-1]
    print(f"\n{'='*60}")
    print(f"Starting scrape of collection: {collection_url}")
    print(f"{'='*60}")
    
    # Get total number of pages
    total_pages = get_total_pages(collection_url)
    
    all_products = []
    seen_urls = set()  # Track seen URLs to avoid duplicates
    seen_titles = set()  # Track seen titles to avoid duplicates
    
    # Scrape each page
    for page_num in range(1, total_pages + 1):
        print(f"\nScraping page {page_num}/{total_pages}")
        
        # Construct page URL
        if page_num == 1:
            page_url = collection_url
        else:
            page_url = f"{collection_url}?page={page_num}"
        
        # Get page content
        content = get_page_content(page_url)
        if not content:
            print(f"Failed to get content for page {page_num}")
            continue
        
        # Extract products from this page
        products = extract_product_links_from_page(content, BASE_URL, get_collection_tags(collection_name))
        
        # If no products found and this is page 2 or later, we might have reached the end
        if not products and page_num > 1:
            print(f"No products found on page {page_num}, stopping pagination")
            break
        
        # Filter out duplicates based on URL and title
        new_products = []
        duplicates_found = 0
        
        for product in products:
            product_url = product['url']
            product_title = product['title'].strip().lower()  # Normalize title for comparison
            
            if product_url in seen_urls:
                print(f"  ⚠ Skipped duplicate URL: {product['title']}")
                duplicates_found += 1
            elif product_title in seen_titles:
                print(f"  ⚠ Skipped duplicate title: {product['title']}")
                duplicates_found += 1
            else:
                seen_urls.add(product_url)
                seen_titles.add(product_title)
                new_products.append(product)
        
        all_products.extend(new_products)
        
        print(f"Found {len(products)} products on page {page_num} ({len(new_products)} new, {duplicates_found} duplicates)")
        
        # Add delay between requests to be respectful
        if page_num < total_pages:
            time.sleep(1)
    
    print(f"\nTotal products extracted from {collection_path}: {len(all_products)} (unique URLs and titles)")
    return {
        'collection_name': collection_name,
        'collection_url': collection_url,
        'total_products': len(all_products),
        'products': all_products
    }

def scrape_all_collection_urls(output_file="all_url.json"):
    """Scrape all collections to get product URLs and save to JSON"""
    all_collections_data = {}
    total_products = 0
    global_seen_urls = set()  # Track URLs across all collections
    global_seen_titles = set()  # Track titles across all collections
    
    # Scrape each collection
    for collection_name, collection_path in COLLECTIONS.items():
        print(f"\n{'='*60}")
        print(f"SCRAPING COLLECTION: {collection_name.upper()}")
        print(f"{'='*60}")
        
        collection_data = scrape_collection_urls(collection_path)
        products = collection_data['products']
        
        # Filter out products that already exist in other collections (by URL or title)
        unique_products = []
        for product in products:
            product_url = product['url']
            product_title = product['title'].strip().lower()  # Normalize title for comparison
            
            # Check if product already exists by URL or title
            if product_url in global_seen_urls:
                print(f"  ⚠ Global duplicate skipped (URL): {product['title']} (already in another collection)")
                continue
            elif product_title in global_seen_titles:
                print(f"  ⚠ Global duplicate skipped (Title): {product['title']} (already in another collection)")
                continue
            else:
                # Add to tracking sets and keep the product
                global_seen_urls.add(product_url)
                global_seen_titles.add(product_title)
                unique_products.append(product)
        
        all_collections_data[collection_name] = {
            'collection_url': collection_data['collection_url'],
            'total_products': len(unique_products),
            'products': unique_products
        }
        
        total_products += len(unique_products)
        
        print(f"Completed {collection_name}: {len(unique_products)} unique products")
    
    # Save all data to JSON file
    data = {
        'base_url': BASE_URL,
        'total_collections': len(all_collections_data),
        'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'collections': all_collections_data
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"Total collections scraped: {len(COLLECTIONS)}")
    print(f"Total unique products extracted: {total_products}")
    print(f"Data saved to {output_file}")
    
    # Print summary for each collection
    for collection_name, data in all_collections_data.items():
        print(f"{collection_name}: {data['total_products']} unique products")
    
    return data

# ===== PRODUCT DATA EXTRACTION MODULE =====
def extract_product_json_from_url(url, max_retries=3):
    """Extract detailed product data from individual product page"""
    for attempt in range(max_retries):
        try:
            print(f"Fetching: {url} (attempt {attempt + 1}/{max_retries})")
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            main_script_tag = soup.find('script', {'type': 'application/json', 'data-product-json': True})
            essential_script_tag = soup.find('script', string=re.compile(r'window\.essentialCountdownTimerMeta'))
            
            combined_data = {}
            
            if main_script_tag:
                main_json_data = json.loads(main_script_tag.string)
                combined_data.update(main_json_data)
            
            if essential_script_tag:
                script_content = essential_script_tag.string
                
                start_pattern = r'productCollections:\s*\['
                start_match = re.search(start_pattern, script_content)
                
                if start_match:
                    start_pos = start_match.end() - 1
                    bracket_count = 0
                    end_pos = start_pos
                    
                    for i in range(start_pos, len(script_content)):
                        char = script_content[i]
                        if char == '[':
                            bracket_count += 1
                        elif char == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                end_pos = i + 1
                                break
                    
                    if bracket_count == 0:
                        collections_json = script_content[start_pos:end_pos]
                        
                        try:
                            collections_json = re.sub(r',\s*}', '}', collections_json)
                            collections_json = re.sub(r',\s*]', ']', collections_json)
                            collections_json = re.sub(r',\s*,', ',', collections_json)
                            collections_json = collections_json.replace('\\/', '/')
                            
                            product_collections = json.loads(collections_json)
                            combined_data['productCollections'] = product_collections
                            print(f"✓ Found {len(product_collections)} collections")
                            
                        except json.JSONDecodeError as e:
                            print(f"Error parsing productCollections: {e}")
                            try:
                                fixed_json = re.sub(r',(\s*[}\]])', r'\1', collections_json)
                                product_collections = json.loads(fixed_json)
                                combined_data['productCollections'] = product_collections
                                print(f"✓ Fixed and found {len(product_collections)} collections")
                            except json.JSONDecodeError as e2:
                                print(f"Still failed to parse after fixing: {e2}")
                    else:
                        print("Could not find matching closing bracket for productCollections")
                else:
                    print("No productCollections found in script")
                
                product_data_match = re.search(r'productData:\s*({.*?}),\s*productCollections', script_content, re.DOTALL)
                if product_data_match:
                    try:
                        product_data_json = product_data_match.group(1)
                        product_data_json = re.sub(r',\s*}', '}', product_data_json)
                        
                        product_data = json.loads(product_data_json)
                        if 'product' in combined_data:
                            combined_data['product'].update(product_data)
                        else:
                            combined_data['product'] = product_data
                    except json.JSONDecodeError as e:
                        print(f"Error parsing productData: {e}")
            
            if combined_data:
                return combined_data
            else:
                print(f"No product JSON found in script tags for: {url}")
                return None
                
        except requests.RequestException as e:
            print(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch {url} after {max_retries} attempts")
                return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON from {url} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                return None
        except Exception as e:
            print(f"Unexpected error processing {url} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                return None
    
    return None

def clean_html_body(html_content):
    """Clean HTML content for product description"""
    if not html_content:
        return ""
    
    html_content = html_content.replace('\n', '<br>')
    soup = BeautifulSoup(html_content, 'html.parser')
    
    for tag in soup.find_all(['div', 'span']):
        tag.unwrap()
    
    allowed_tags = ['p', 'br']
    for tag in soup.find_all():
        if tag.name not in allowed_tags:
            tag.replace_with(tag.get_text())
    
    cleaned_html = str(soup)
    cleaned_html = re.sub(r'\s+', ' ', cleaned_html)
    cleaned_html = re.sub(r'<br>\s*<br>', '<br><br>', cleaned_html)
    cleaned_html = re.sub(r'<p>\s*</p>', '', cleaned_html)
    
    return cleaned_html.strip()

def format_image_url(image_url):
    """Format image URL to be absolute"""
    if not image_url:
        return ""
    
    if image_url.startswith('http'):
        return image_url
    
    if image_url.startswith('//'):
        return f"https:{image_url}"
    
    if image_url.startswith('/'):
        return f"https://notorious-plug.com{image_url}"
    
    return image_url

def structure_product_data(raw_product_data, source_url, additional_tags=None):
    """Structure raw product data into organized format"""
    try:
        # If raw_product_data is a string, try to parse it as JSON
        if isinstance(raw_product_data, str):
            try:
                raw_product_data = json.loads(raw_product_data)
            except json.JSONDecodeError as e:
                print(f"Failed to parse string as JSON: {e}")
                return None
        
        # Ensure raw_product_data is a dictionary
        if not isinstance(raw_product_data, dict):
            print(f"raw_product_data is not a dict, it's {type(raw_product_data)}")
            return None
        
        product = raw_product_data.get('product', {})
        handle = product.get('handle', '')
        
        collection_tags = []
        first_collection_handle = None
        if 'productCollections' in raw_product_data:
            for i, collection in enumerate(raw_product_data['productCollections']):
                if isinstance(collection, dict):
                    collection_title = collection.get('title', '')
                    collection_handle = collection.get('handle', '')
                    
                    if collection_title:
                        collection_tags.append(collection_title)
                    
                    if i == 0 and collection_handle:
                        first_collection_handle = collection_handle
        
        print(f"Extracted collection tags: {collection_tags}")
        print(f"First collection handle: {first_collection_handle}")
        
        existing_tags = product.get('tags', [])
        all_tags = existing_tags + collection_tags
        
        if additional_tags:
            all_tags.extend(additional_tags)
        
        description_content = product.get('description', '')
        try:
            cleaned_body = clean_html_body(description_content)
        except Exception as e:
            print(f"Error in clean_html_body: {e}")
            cleaned_body = ""
        
        structured_data = {
            'handle': handle,
            'base': {
                'Handle': handle,
                'Title': product.get('title', ''),
                'Body (HTML)': f"<p>{cleaned_body}</p>" if cleaned_body else "",
                'Vendor': product.get('vendor', ''),
                'Product Category': first_collection_handle or '',
                'Type': product.get('type', ''),
                'Tags': ', '.join(filter(None, all_tags)),
                'Published': 'TRUE',
                'Option1 Name': 'Size',
                'Option1 Value': '',
                'Option2 Name': '',
                'Option2 Value': '',
                'Option3 Name': '',
                'Option3 Value': '',
                'Variant SKU': '',
                'Variant Grams': '',
                'Variant Inventory Tracker': 'shopify',
                'Variant Inventory Qty': '',
                'Variant Inventory Policy': 'deny',
                'Variant Fulfillment Service': 'manual',
                'Variant Price': '',
                'Variant Compare At Price': '',
                'Variant Requires Shipping': 'TRUE',
                'Variant Taxable': 'TRUE',
                'Variant Barcode': '',
                'Image Src': '',
                'Image Position': '',
                'Image Alt Text': '',
                'Gift Card': 'FALSE',
                'SEO Title': product.get('title', ''),
                'SEO Description': cleaned_body[:160] if cleaned_body else '',
                'Google Shopping / Google Product Category': '',
                'Google Shopping / Gender': '',
                'Google Shopping / Age Group': '',
                'Google Shopping / MPN': '',
                'Google Shopping / AdWords Grouping': '',
                'Google Shopping / AdWords Labels': '',
                'Google Shopping / Condition': 'new',
                'Google Shopping / Custom Product': 'FALSE',
                'Google Shopping / Custom Label 0': '',
                'Google Shopping / Custom Label 1': '',
                'Google Shopping / Custom Label 2': '',
                'Google Shopping / Custom Label 3': '',
                'Google Shopping / Custom Label 4': '',
                'Variant Image': '',
                'Variant Weight Unit': 'kg',
                'Variant Tax Code': '',
                'Cost per item': '',
                'Status': 'active'
            },
            'variants': [],
            'images': {},
            'collections': raw_product_data.get('productCollections', []),
            'source_url': source_url
        }
        
        # Process variants
        variants = product.get('variants', [])
        for i, variant in enumerate(variants):
            if not isinstance(variant, dict):
                continue
                
            try:
                variant_data = {
                    'id': variant.get('id', ''),
                    'title': variant.get('title', ''),
                    'option1': variant.get('option1', ''),
                    'option2': variant.get('option2', ''),
                    'option3': variant.get('option3', ''),
                    'sku': variant.get('sku', ''),
                    'requires_shipping': variant.get('requires_shipping', True),
                    'taxable': variant.get('taxable', True),
                    'featured_image': variant.get('featured_image'),
                    'available': variant.get('available', True),
                    'price': float(variant.get('price', 0)) / 100,
                    'grams': variant.get('grams', 0),
                    'compare_at_price': float(variant.get('compare_at_price', 0)) / 100 if variant.get('compare_at_price') else 0,
                    'position': variant.get('position', 1),
                    'inventory_policy': 'deny',
                    'inventory_management': variant.get('inventory_management', 'shopify'),
                    'inventory_quantity': variant.get('inventory_quantity', 0),
                    'weight': variant.get('weight', 0),
                    'weight_unit': variant.get('weight_unit', 'kg'),
                    'old_inventory_quantity': variant.get('old_inventory_quantity', 0),
                    'requires_shipping': variant.get('requires_shipping', True),
                    'admin_graphql_api_id': variant.get('admin_graphql_api_id', '')
                }
                structured_data['variants'].append(variant_data)
            except Exception as e:
                print(f"Error processing variant {i}: {e}")
        
        # Process images
        images = product.get('images', [])
        for i, image in enumerate(images):
            try:
                image_position = str(i + 1)
                if isinstance(image, dict):
                    image_url = format_image_url(image.get('src', ''))
                elif isinstance(image, str):
                    image_url = format_image_url(image)
                else:
                    continue
                    
                if image_url:
                    if image_position not in structured_data['images']:
                        structured_data['images'][image_position] = []
                    structured_data['images'][image_position].append(image_url)
            except Exception as e:
                print(f"Error processing image {i}: {e}")
        
        return structured_data
        
    except Exception as e:
        print(f"Error structuring product data: {e}")
        return None

def get_product_urls_from_json(json_file="all_url.json", limit=None):
    """Get product URLs from the scraped JSON file"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        product_urls = []
        for collection_name, collection_data in data.get('collections', {}).items():
            for product in collection_data.get('products', []):
                product_urls.append({
                    'url': product['url'],
                    'title': product['title'],
                    'collection': collection_name,
                    'tags': product.get('tags', [])
                })
        
        if limit:
            product_urls = product_urls[:limit]
        
        print(f"Found {len(product_urls)} product URLs to process")
        return product_urls
        
    except FileNotFoundError:
        print(f"File {json_file} not found. Please run scrape_all_collection_urls() first.")
        return []
    except Exception as e:
        print(f"Error reading product URLs from {json_file}: {e}")
        return []

def extract_detailed_product_data(product_urls, output_file="extracted_product_json.json", batch_size=10, delay=2):
    """Extract detailed product data from URLs and save to JSON"""
    all_structured_data = {'products': {}}
    processed_count = 0
    
    print(f"Starting detailed extraction of {len(product_urls)} products...")
    
    for i, product_info in enumerate(product_urls):
        url = product_info['url']
        collection_tags = product_info.get('tags', [])
        
        print(f"\n[{i+1}/{len(product_urls)}] Processing: {product_info['title']}")
        
        raw_data = extract_product_json_from_url(url)
        
        if raw_data:
            structured_data = structure_product_data(raw_data, url, collection_tags)
            
            if structured_data:
                handle = structured_data['handle']
                all_structured_data['products'][handle] = structured_data
                processed_count += 1
                print(f"✅ Successfully processed: {handle}")
            else:
                print(f"❌ Failed to structure data for: {url}")
        else:
            print(f"❌ No data extracted from: {url}")
        
        # Save progress periodically
        if (i + 1) % batch_size == 0 or i == len(product_urls) - 1:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_structured_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Progress saved: {processed_count}/{i+1} products processed")
        
        # Be polite to the server
        if i < len(product_urls) - 1:
            time.sleep(delay)
    
    print(f"\n🎉 Detailed extraction completed!")
    print(f"📄 Data saved to: {output_file}")
    print(f"📊 Successfully processed: {processed_count}/{len(product_urls)} products")
    
    return all_structured_data

def save_json_response(data, filename="response_data.json"):
    """Save API response to JSON file"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"💾 Data saved to {filename}")

# ===== DATA CLEANING MODULE =====
def clean_and_save_product_data(raw_json_file="extracted_product_json.json", cleaned_json_file="cleaned_products.json"):
    """Clean and format product data to match the exact specified structure."""
    with open(raw_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    products_data = data.get("products", {})
    cleaned_products = []

    for handle, product_data in products_data.items():
        base_data = product_data.get("base", {})
        variants = product_data.get("variants", [])
        images = product_data.get("images", {})
        
        # Extract basic product information
        title = base_data.get("Title", "")
        description = base_data.get("Body (HTML)", "")
        vendor = base_data.get("Vendor", "")
        product_category = base_data.get("Product Category", "")
        product_type = base_data.get("Type", "")
        tags = base_data.get("Tags", "")

        # Process variants
        cleaned_variants = []
        seen_variants = set()  # To avoid duplicates
        
        for variant in variants:
            sku = variant.get("sku", "")
            size = variant.get("option1", "") or variant.get("title", "")
            color = variant.get("option2", "") or ""  # Notorious doesn't have explicit color variants
            price = variant.get("price", 0)
            compare_price = variant.get("compare_at_price", 0)
            
            # Create unique identifier for variant
            variant_key = (sku, size, color)
            if variant_key in seen_variants:
                continue
            seen_variants.add(variant_key)
            
            # Get images for this variant - use all available images
            variant_images = []
            if images:
                # Use all images from the product for each variant
                for position, image_urls in images.items():
                    if isinstance(image_urls, list):
                        variant_images.extend(image_urls)
                    else:
                        variant_images.append(image_urls)
            
            cleaned_variant = {
                "Variant SKU": sku,
                "size": size,
                "color": color,
                "Variant Price": price,
                "Variant Compare At Price": compare_price,
                "images": variant_images
            }
            
            cleaned_variants.append(cleaned_variant)

        # Only add product if it has variants
        if cleaned_variants:
            cleaned_product = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": vendor,
                "Product Category": product_category,
                "Type": product_type,
                "Tags": tags,
                "variants": cleaned_variants
            }
            
            cleaned_products.append(cleaned_product)

    # Save cleaned data in the exact format specified
    final_output = {"products": cleaned_products}
    
    with open(cleaned_json_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)
    
    print(f"[✓] Cleaned product data saved to {cleaned_json_file}")
    print(f"[✓] Total products processed: {len(cleaned_products)}")
    print(f"[✓] Total variants: {sum(len(p['variants']) for p in cleaned_products)}")
    
    return final_output

# ===== MAIN WORKFLOW FUNCTIONS =====
def scrape_and_save_product_urls(output_file="all_url.json"):
    """Complete workflow to scrape and save product URLs"""
    urls_data = scrape_all_collection_urls(output_file)
    return urls_data

def extract_and_save_detailed_data(url_file="all_url.json", output_file="extracted_product_json.json", limit=None, batch_size=10, delay=2):
    """Complete workflow to extract and save detailed product data"""
    product_urls = get_product_urls_from_json(url_file, limit)
    if not product_urls:
        return None
    
    detailed_data = extract_detailed_product_data(product_urls, output_file, batch_size, delay)
    return detailed_data

def complete_workflow(limit=None, output_file="shopify_products.csv"):
    """Run the complete workflow: URL scraping, detailed extraction, and processing data"""
    # Step 1: Scrape product URLs from all collections
    print("=== STEP 1: Scraping Product URLs ===")
    urls_data = scrape_and_save_product_urls()
    
    # Step 2: Extract detailed product data
    print("\n=== STEP 2: Extracting Detailed Product Data ===")
    detailed_data = extract_and_save_detailed_data(limit=limit)
    
    # Step 3: Clean and save product data
    print("\n=== STEP 3: Cleaning and Processing Data ===")
    if detailed_data:
        cleaned_products = clean_and_save_product_data()
        upsert_product(cleaned_products, BASE_URL, "pound")
        print(f"[✓] Complete workflow finished successfully! Data processed and saved to database")
    else:
        print("[❌] Workflow failed at the detailed data extraction step.")

# Run the script if executed directly
if __name__ == "__main__":
    # Example usage - limit to 20 products for testing, remove limit for full scrape
    complete_workflow()