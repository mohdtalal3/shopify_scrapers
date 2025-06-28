import json
import requests
from bs4 import BeautifulSoup
import time
import re
import csv
from urllib.parse import urljoin

def extract_product_json_from_url(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            print(f"Fetching: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=30)
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
                            
                            collection_titles = [col.get('title', '') for col in product_collections if col.get('title')]
                            print(f"Collection titles: {', '.join(collection_titles)}")
                            
                        except json.JSONDecodeError as e:
                            print(f"Error parsing productCollections: {e}")
                            try:
                                fixed_json = re.sub(r',(\s*[}\]])', r'\1', collections_json)
                                product_collections = json.loads(fixed_json)
                                combined_data['productCollections'] = product_collections
                                print(f"✓ Fixed and found {len(product_collections)} collections")
                                
                                collection_titles = [col.get('title', '') for col in product_collections if col.get('title')]
                                print(f"Collection titles: {', '.join(collection_titles)}")
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
    try:
        product = raw_product_data.get('product', {})
        handle = product.get('handle', '')
        
        collection_tags = []
        first_collection_handle = None
        if 'productCollections' in raw_product_data:
            for i, collection in enumerate(raw_product_data['productCollections']):
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
        
        seen_tags = set()
        unique_tags = []
        for tag in all_tags:
            if tag not in seen_tags:
                seen_tags.add(tag)
                unique_tags.append(tag)
        
        print(f"All unique tags before processing: {unique_tags}")
        
        clothing_categories = [
            'bottoms', 't-shirts', 'sweats', 'jeans', 'shirts', 'shoes', 'bags',
            'jackets', 'hoodies', 'sweaters', 'dresses', 'skirts', 'tops',
            'pants', 'trousers', 'shorts', 'blazers', 'coats', 'outerwear',
            'footwear', 'sneakers', 'boots', 'sandals', 'handbags', 'backpacks',
            'wallets', 'belts', 'jewelry', 'watches', 'sunglasses', 'hats',
            'scarves', 'gloves', 'socks', 'underwear', 'lingerie', 'swimwear',
            'activewear', 'sportswear', 'formal', 'casual', 'streetwear', 'hoodie','shirt',
            'jacket','pant','trouser','short','coat','assessories','clothing'
        ]
        
        type_from_tags = None
        for category in clothing_categories:
            for tag in unique_tags:
                tag_lower = tag.lower()
                print(f"Checking category '{category}' against tag: {tag} (lower: {tag_lower})")
                if category in tag_lower:
                    type_from_tags = category.title()
                    print(f"Found category '{category}' in tag '{tag}', setting type to '{type_from_tags}'")
                    break
            if type_from_tags:
                break
        
        if not type_from_tags:
            print("No clothing category found in tags, checking collection titles...")
            for category in clothing_categories:
                for collection_title in collection_tags:
                    collection_lower = collection_title.lower()
                    print(f"Checking category '{category}' against collection: {collection_title} (lower: {collection_lower})")
                    if category in collection_lower:
                        type_from_tags = category.title()
                        print(f"Found category '{category}' in collection '{collection_title}', setting type to '{type_from_tags}'")
                        break
                if type_from_tags:
                    break
        
        processed_tags = []
        has_accessories = False
        has_womens = False
        has_mens = False
        
        for tag in unique_tags:
            tag_lower = tag.lower()
            
            if 'accessories' in tag_lower or 'bags' in tag_lower:
                has_accessories = True
            
            if 'women' in tag_lower or 'womens' in tag_lower:
                has_womens = True
            if 'men' in tag_lower or 'mens' in tag_lower:
                has_mens = True
            
            if tag_lower == "all men":
                if has_accessories:
                    processed_tags.append("All Mens Accessories")
                else:
                    processed_tags.append("All Mens Clothing")
            elif tag_lower == "all women":
                if has_accessories:
                    processed_tags.append("All Womens Accessories")
                else:
                    processed_tags.append("All Womens Clothing")
            else:
                processed_tags.append(tag)
        
        if has_womens and not any("womens" in tag.lower() for tag in processed_tags):
            if has_accessories:
                processed_tags.append("All Womens Accessories")
            else:
                processed_tags.append("All Womens Clothing")
        
        if has_mens and not any("mens" in tag.lower() for tag in processed_tags):
            if has_accessories:
                processed_tags.append("All Mens Accessories")
            else:
                processed_tags.append("All Mens Clothing")
        
        print(f"Processed tags: {processed_tags}")
        print(f"Type from tags: {type_from_tags}")
        
        tags_string = ", ".join(processed_tags) if processed_tags else ""
        
        raw_body = product.get('content', '')
        cleaned_body = clean_html_body(raw_body)
        
        if type_from_tags:
            type_value = type_from_tags
        elif first_collection_handle:
            type_value = first_collection_handle
        else:
            type_value = handle
        
        structured_product = {
            "base": {
                "Handle": handle,
                "Title": product.get('title', ''),
                "Body (HTML)": cleaned_body,
                "Vendor": product.get('vendor', ''),
                "Product Category": f"{product.get('type', '')}",
                "Type": type_value,
                "Tags": tags_string,
                "Published": "TRUE",
                "Status": "active",
                "Variant Inventory Policy": "deny",
                "Variant Fulfillment Service": "manual"
            },
            "variants": [],
            "images": {},
            "collections": collection_tags,
            "source_url": source_url,
            "extracted_at": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        variants = product.get('variants', [])
        for variant in variants:
            variant_data = {
                "sku": variant.get('sku', ''),
                "size": variant.get('option1', ''),
                "color": variant.get('option2', ''),
                "price": variant.get('price', 0) / 100,
                "compare_price": variant.get('compare_at_price', 0) / 100,
                "variant_id": variant.get('id', ''),
                "title": variant.get('title', ''),
                "available": variant.get('available', False),
                "inventory_management": variant.get('inventory_management', ''),
                "weight": None,
                "grams": None,
                "inventory_quantity": None,
                "weight_unit": None
            }
            structured_product["variants"].append(variant_data)
        
        images = product.get('images', [])
        media = product.get('media', [])
        
        if media:
            for media_item in media:
                if media_item.get('media_type') == 'image':
                    image_url = media_item.get('src', '')
                    if image_url:
                        formatted_url = format_image_url(image_url)
                        position = media_item.get('position', 1)
                        if position not in structured_product["images"]:
                            structured_product["images"][position] = []
                        structured_product["images"][position].append(formatted_url)
        else:
            for i, image_url in enumerate(images, 1):
                if i not in structured_product["images"]:
                    structured_product["images"][i] = []
                formatted_url = format_image_url(image_url)
                structured_product["images"][i].append(formatted_url)
        
        return structured_product, unique_tags
        
    except Exception as e:
        print(f"Error structuring product data: {e}")
        return None, []

def get_product_urls_from_json(json_file="all_url.json", limit=None):
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        product_data = []
        
        for collection_name, collection_data in data.get('collections', {}).items():
            products = collection_data.get('products', [])
            for product in products:
                if limit and len(product_data) >= limit:
                    break
                product_data.append({
                    'url': product['url'],
                    'tags': product.get('tags', [])
                })
            
            if limit and len(product_data) >= limit:
                break
        
        return product_data if limit is None else product_data[:limit]
        
    except FileNotFoundError:
        print(f"Error: {json_file} not found")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing {json_file}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error reading {json_file}: {e}")
        return []

def save_unique_tags_to_csv(all_unique_tags, filename="tags.csv"):
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Tag'])
            for tag in sorted(all_unique_tags):
                writer.writerow([tag])
        print(f"✓ Saved {len(all_unique_tags)} unique tags to {filename}")
    except Exception as e:
        print(f"Error saving tags to CSV: {e}")

def main():
    print("Starting product JSON extraction...")
    
    product_data_list = get_product_urls_from_json(limit=None)
    
    if not product_data_list:
        print("No product URLs found. Exiting.")
        return
    
    print(f"Found {len(product_data_list)} product URLs to process")
    
    extracted_products = {}
    all_unique_tags = set()
    
    for i, product_data in enumerate(product_data_list, 1):
        url = product_data['url']
        tags = product_data['tags']
        print(f"\nProcessing product {i}/{len(product_data_list)}")
        print(f"URL: {url}")
        print(f"Tags: {', '.join(tags) if tags else 'None'}")
        
        raw_product_data = extract_product_json_from_url(url)
        
        if raw_product_data:
            structured_product, unique_tags = structure_product_data(raw_product_data, url, tags)
            
            if structured_product:
                handle = structured_product["base"]["Handle"]
                extracted_products[handle] = structured_product
                
                all_unique_tags.update(unique_tags)
                
                print(f"✓ Successfully extracted data for: {structured_product['base']['Title']}")
                print(f"  Final tags: {structured_product['base']['Tags']}")
                print(f"  Type (handle): {structured_product['base']['Type']}")
                print(f"  Images: {len(structured_product['images'])} image sets")
            else:
                print(f"✗ Failed to structure data from: {url}")
        else:
            print(f"✗ Failed to extract data from: {url}")
        
        if i < len(product_data_list):
            time.sleep(2)
    
    output_file = "extracted_product_json.json"
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total_products': len(extracted_products),
                'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'products': extracted_products
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Successfully extracted {len(extracted_products)} products")
        print(f"✓ Data saved to: {output_file}")
        
        save_unique_tags_to_csv(all_unique_tags)
        
        print("\nExtracted Products Summary:")
        for i, (handle, product) in enumerate(extracted_products.items(), 1):
            title = product['base']['Title']
            vendor = product['base']['Vendor']
            tags = product['base']['Tags']
            type_handle = product['base']['Type']
            variants_count = len(product['variants'])
            images_count = len(product['images'])
            print(f"{i}. {title} - {vendor} - Type: {type_handle} - Tags: {tags} - {variants_count} variants, {images_count} image sets")
            
    except Exception as e:
        print(f"Error saving data to {output_file}: {e}")

if __name__ == "__main__":
    main() 
