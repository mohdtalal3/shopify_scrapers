import requests
import json
from bs4 import BeautifulSoup
import re
import os
from dotenv import load_dotenv
import sys

# Add parent directory to path for db import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import upsert_all_product_data

# Load environment variables
load_dotenv()

# Constants
BASE_URL = "https://www.marcjacobs.com"
GRID_URL = "https://www.marcjacobs.com/on/demandware.store/Sites-mjsfra-Site/en_US/Search-UpdateGrid"
PRODUCT_URL = "https://www.marcjacobs.com/on/demandware.store/Sites-mjsfra-Site/en_US/Product-Variation"

# Proxy configuration
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None


def clean_product_data(data, gender_tag=None, product_type_override=None):
    """
    Cleans and formats product data from a given dictionary.
    
    Args:
        data (dict): The product data in JSON format
        gender_tag (str, optional): A gender tag to apply to the product category
        product_type_override (str, optional): An override for the product type
    
    Returns:
        list: A list of dictionaries representing cleaned products with variants
    """
    if not data:
        return []

    # Extract basic product information
    handle = data.get("id")
    title = data.get("productName", "")
    cleaned_title = re.sub(r'[^A-Za-z0-9 ]+', '', title)
    formatted_title = cleaned_title.title()
    description = data.get("longDescription") or ""
    brand = data.get("brand", "")
    
    # Build product tags
    product_tags = []
    if gender_tag:
        product_tags.extend([gender_tag.lower(), f"{gender_tag.lower()}s", f"{gender_tag.lower()}'s"])
    
    # Add categories and other relevant info as tags
    for field in ["productParentCategory", "productCategory", "productType", "brand", "selectedColorValue"]:
        value = data.get(field)
        if value:
            product_tags.append(str(value).lower())
    
    # Add material tag if available
    if data.get("custom", {}).get("material"):
        product_tags.append(data["custom"]["material"].lower())
    
    # Get size information
    overall_product_size_display = ""
    for attr in data.get("variationAttributes", []):
        if attr.get("attributeId") == "size":
            overall_product_size_display = attr.get("displayValue", "")
            if overall_product_size_display:
                product_tags.append(overall_product_size_display.lower())
            break

    # Clean and format tags
    product_tags = list(set(tag.strip() for tag in product_tags if tag.strip()))
    product_tags_str = ", ".join(product_tags)

    # Set category and type
    category_val = gender_tag.lower() if gender_tag else data.get("productParentCategory", "")
    type_val = "Tote"

    # Extract images
    all_images = []
    for img in data.get("images", {}).get("large", []):
        url = img.get("url")
        if url:
            all_images.append(url)

    # Create product entry
    product_entry = {
        "Handle": handle,
        "Title": formatted_title,
        "Body (HTML)": description,
        "Vendor": brand,
        "Product Category": category_val,
        "Type": type_val,
        "Tags": product_tags_str,
        "variants": []
    }

    # Get pricing information
    selected_price = data.get("price", {}).get("sales", {}).get("value", 0)
    selected_compare_price = data.get("price", {}).get("list", {}).get("value", 0) if data.get("price", {}).get("list") else 0
    current_selected_color_display = data.get("selectedColorValue")
    master_id = data.get("masterID", "")

    # Add main variant
    product_entry["variants"].append({
        "Variant SKU": data.get("id"),
        "size": overall_product_size_display,
        "color": current_selected_color_display,
        "Variant Price": selected_price,
        "Variant Compare At Price": selected_compare_price,
        "images": all_images
    })

    # Add color variants
    for attr in data.get("variationAttributes", []):
        if attr.get("attributeId") == "color":
            for val in attr.get("values", []):
                if not val.get("selected") and val.get("selectable"):
                    variant_id = val.get("id", "")
                    constructed_sku = f"{master_id}-{variant_id}" if master_id else variant_id
                    product_entry["variants"].append({
                        "Variant SKU": constructed_sku,
                        "size": overall_product_size_display,
                        "color": val.get("displayValue"),
                        "Variant Price": selected_price,
                        "Variant Compare At Price": selected_compare_price,
                        "images": all_images
                    })

    return [product_entry]


def process_product_data(data, output_file_path="cleaned_product_data.json", gender_tag=None, product_type=None):
    """Process and save cleaned product data."""
    try:
        cleaned_results = []
        
        for idx, product_data in enumerate(data, 1):
            try:
                cleaned = clean_product_data(product_data, gender_tag, product_type)
                if cleaned:
                    cleaned_results.extend(cleaned)
            except Exception as e:
                print(f"Error processing product #{idx}: {e}")
        
        # # # Save to file
        # with open(output_file_path, 'w', encoding='utf-8') as outfile:
        #     json.dump(cleaned_results, outfile, indent=4, ensure_ascii=False)
        
        # Save to database
        upsert_all_product_data(cleaned_results, BASE_URL, "USD")
        print(f"Cleaned {len(cleaned_results)} products saved to {output_file_path}")
        
    except Exception as e:
        print(f"Error processing data: {e}")


def extract_product_ids(html_content):
    """Extract product IDs from grid HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    ids = set()

    # Extract IDs from button attributes
    for button in soup.find_all('button', attrs={'data-product-id': True}):
        ids.add(button['data-product-id'])

    # Remove last segment after '-'
    processed = []
    for pid in ids:
        parts = str(pid).split('-')
        if len(parts) > 1:
            pid = '-'.join(parts[:-1])
        processed.append(pid)
    
    return sorted(set(processed))


def fetch_grid_page(cgid, start=0, sz=18, pgNo=1):
    """Fetch product grid page."""
    params = {
        "cgid": cgid,
        "start": start,
        "sz": sz,
        "pgNo": pgNo,
        "enableInfiniteScroll": "true"
    }
    response = requests.get(GRID_URL, params=params)
    response.raise_for_status()
    return response.text


def fetch_product(pid):
    """Fetch individual product data."""
    params = {
        "pid": pid,
        "quantity": 1,
        "isQuickView": "false",
        "isEditCart": "false"
    }
    response = requests.get(PRODUCT_URL, params=params, proxies=proxies)
    response.raise_for_status()
    data = response.json()
    return data.get("product", {})


def scrape_marc_jacobs_products():
    """Main workflow to scrape Marc Jacobs products."""
    cgid = "The-Leather-Tote-Bag"
    all_products = []
    last_batch = None
    start = 0
    pgNo = 1
    page_size = 100

    print("Starting Marc Jacobs product scraping...")
    
    while True:
        try:
            html = fetch_grid_page(cgid, start=start, sz=page_size, pgNo=pgNo)
            batch_ids = extract_product_ids(html)
            
            if not batch_ids:
                print("No IDs found, stopping.")
                break

            if batch_ids == last_batch:
                print("Batch repeated, stopping.")
                break
                
            last_batch = batch_ids
            
            for pid in batch_ids:
                try:
                    product_data = fetch_product(pid)
                    if product_data:
                        all_products.append(product_data)
                        print(f"Fetched product: {product_data.get('productName', 'Unknown')}")
                except Exception as e:
                    print(f"Failed to fetch product {pid}: {e}")

            start += page_size
            pgNo += 1
            
        except Exception as e:
            print(f"Error fetching grid page: {e}")
            break

    # Process and save results
    if all_products:
        process_product_data(all_products, "cleaned_output.json", "Women")
        print(f"Successfully processed {len(all_products)} products.")
    else:
        print("No products were fetched.")


if __name__ == "__main__":
    scrape_marc_jacobs_products()
