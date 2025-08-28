import json
import logging
import sys
import requests
import urllib.parse
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from db import upsert_all_product_data
BASE_URL="https://www.boohoo.com"
# Load environment variables
load_dotenv()
proxy_str = os.getenv("PROXY_URL")
proxies = {"http": proxy_str, "https": proxy_str} if proxy_str else None

# Set up logging for debugging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def fetch_products_from_algolia(page):
    url = "https://hnc30iyynp-3.algolianet.com/1/indexes/*/queries"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8,af;q=0.7,ar;q=0.6,be;q=0.5,de;q=0.4",
        "content-type": "application/x-www-form-urlencoded",
        "x-algolia-api-key": "6e5de83d201b6bdaac45d449a9466a42",
        "x-algolia-application-id": "HNC30IYYNP",
        "x-algolia-agent": "Algolia for JavaScript (4.23.3); Browser; JS Helper (3.15.0)",
        "origin": "https://www.boohoo.com",
        "referer": "https://www.boohoo.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }
    
    # Base payload from the provided request
    base_params = "analyticsTags=%5B%22category-search%22%2C%22facet-refinement%22%5D&clickAnalytics=true&enablePersonalization=true&facetFilters=%5B%22categorySlugs%3Abrands-boohoo%22%2C%5B%22categoryTaxonomy%3ACo-ords%22%2C%22categoryTaxonomy%3ADresses%22%2C%22categoryTaxonomy%3AHoodies%20%26%20Sweatshirts%22%2C%22categoryTaxonomy%3AJumpers%20%26%20Cardigans%22%2C%22categoryTaxonomy%3AJumpsuits%22%2C%22categoryTaxonomy%3ALeggings%22%2C%22categoryTaxonomy%3ALoungewear%22%2C%22categoryTaxonomy%3AShirts%22%2C%22categoryTaxonomy%3AShorts%22%2C%22categoryTaxonomy%3AShorts%20Co-ords%22%2C%22categoryTaxonomy%3ASkirt%20Co-ords%22%2C%22categoryTaxonomy%3ASkirts%22%2C%22categoryTaxonomy%3ASuits%22%2C%22categoryTaxonomy%3AT-Shirts%22%2C%22categoryTaxonomy%3ATops%22%2C%22categoryTaxonomy%3ATracksuits%22%2C%22categoryTaxonomy%3ATrouser%20Co-ords%22%2C%22categoryTaxonomy%3ATrousers%22%5D%5D&facetingAfterDistinct=true&facets=%5B%22aiattributes.Arm_Style%22%2C%22aiattributes.Back_Exposure%22%2C%22aiattributes.Back_Style%22%2C%22aiattributes.Band%22%2C%22aiattributes.Band_Size%22%2C%22aiattributes.Band_Thickness%22%2C%22aiattributes.Base_Type%22%2C%22aiattributes.Benefit%22%2C%22aiattributes.Bra_Cup_Details%22%2C%22aiattributes.Care%22%2C%22aiattributes.Closure%22%2C%22aiattributes.Colour_Temperature%22%2C%22aiattributes.Comfort_Type%22%2C%22aiattributes.Concern%22%2C%22aiattributes.Coverage%22%2C%22aiattributes.Cuff_Details%22%2C%22aiattributes.Denim_Wash%22%2C%22aiattributes.Density%22%2C%22aiattributes.Embellishment%22%2C%22aiattributes.Fabric_Content%22%2C%22aiattributes.Fabric_Functionality%22%2C%22aiattributes.Fill_Material%22%2C%22aiattributes.Finish%22%2C%22aiattributes.Fit%22%2C%22aiattributes.Flower_Species%22%2C%22aiattributes.Fragrance_Family%22%2C%22aiattributes.Frame_Style%22%2C%22aiattributes.Functionality%22%2C%22aiattributes.Hair_Type%22%2C%22aiattributes.Hanging_Style%22%2C%22aiattributes.Heat_Compatibility%22%2C%22aiattributes.Heel_Height%22%2C%22aiattributes.Heel_Style%22%2C%22aiattributes.Home_Setting%22%2C%22aiattributes.Home_Style%22%2C%22aiattributes.Impact_Level%22%2C%22aiattributes.Ingredient_Preference%22%2C%22aiattributes.Ingredients%22%2C%22aiattributes.Key_Notes%22%2C%22aiattributes.Lash_Band%22%2C%22aiattributes.Lash_Length%22%2C%22aiattributes.Lash_Material%22%2C%22aiattributes.Lash_Shape%22%2C%22aiattributes.Length%22%2C%22aiattributes.Lens_Specialty%22%2C%22aiattributes.Lens_Style%22%2C%22aiattributes.Light_Filtration%22%2C%22aiattributes.Lining_Type%22%2C%22aiattributes.Material%22%2C%22aiattributes.Mattress_Type%22%2C%22aiattributes.Metal%22%2C%22aiattributes.Neck_Style%22%2C%22aiattributes.Pattern%22%2C%22aiattributes.Plant_Species%22%2C%22aiattributes.Print%22%2C%22aiattributes.Seat_Style%22%2C%22aiattributes.Seating_Capacity%22%2C%22aiattributes.Shade_Shape%22%2C%22aiattributes.Skin_Tone%22%2C%22aiattributes.Skin_Type%22%2C%22aiattributes.Sleeve_Length%22%2C%22aiattributes.Special_Occasion%22%2C%22aiattributes.Strap_Details%22%2C%22aiattributes.Waist_Rise%22%2C%22aiattributes.Wax_Type%22%2C%22aiattributes.Wick_Count%22%2C%22aiattributes.Window_Blinds_Style%22%2C%22brand%22%2C%22categorySlugs%22%2C%22categoryTaxonomy%22%2C%22classification%22%2C%22collection%22%2C%22colourFacets%22%2C%22department%22%2C%22design%22%2C%22detail%22%2C%22discountBands%22%2C%22discountRange%22%2C%22fabrication%22%2C%22gender%22%2C%22isNextDay%22%2C%22neckline%22%2C%22occasion%22%2C%22priceBands%22%2C%22priceRange%22%2C%22productKey%22%2C%22ratingGroup%22%2C%22sizes%22%2C%22sleeveLength%22%2C%22source%22%2C%22styleTaxonomy%22%5D&getRankingInfo=true&hitsPerPage=40&ruleContexts=%5B%22category-search%22%5D&tagFilters=&userToken="
    
    # Update page parameter
    params = f"{base_params}&page={page}"
    payload = {
        "requests": [
            {
                "indexName": "boohooww-dbz-prod-price-asc",
                "params": params
            },
            {
                "indexName": "boohooww-dbz-prod-price-asc",
                "params": "analytics=false&analyticsTags=%5B%22category-search%22%2C%22facet-refinement%22%5D&clickAnalytics=false&enablePersonalization=true&facetFilters=%5B%22categorySlugs%3Abrands-boohoo%22%5D&facetingAfterDistinct=true&facets=categoryTaxonomy&getRankingInfo=true&hitsPerPage=0&page=0&ruleContexts=%5B%22category-search%22%5D&userToken="
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, proxies=proxies)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch page {page}: {str(e)}")
        return None

def clean_and_save_product_data_only_available_with_all_images_from_data(
    data, gender_tag=None, product_type=None
):
    # Extract products from the data structure
    products = data.get("results", [{}])[0].get("hits", [])
    cleaned_products = {}

    for product in products:
        if product is None:
            continue

        # Check if product is available for sale
        if not product.get("isOnStock", True):
            continue

        # Extract key fields
        handle = product.get("slug")
        title = product.get("name")
        description = product.get("description", f"<p>{product.get('description', '')}</p>")
        brand = product.get("brand", "")
        product_tags = list(set(product.get("categoryNames", [])))
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}

        type_val = product.get("categoryTaxonomy")
        all_tags = product_tags + list(gender_tags)
        if type_val:
            all_tags.extend(type_val.split())
        product_tags = ", ".join(tag.strip() for tag in all_tags if tag.strip())

        # Extract all images
        all_images = []
        seen_images = set()
        for url in product.get("images", []):
            if url and url not in seen_images:
                all_images.append(url)
                seen_images.add(url)

        # Category is just gender
        category_val = gender_tag.lower() if gender_tag else ""

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category_val,
                "Type": type_val,
                "Tags": product_tags,
                "variants": []
            }

        seen = set()
        for variant in product.get("colourVariants", []):
            if not variant.get("isOnStock", False):
                continue

            sku = variant.get("sku", "")
            color = variant.get("colour", "")
            sizes = variant.get("sizesInStock", [])

            # Get pricing information from pricesBySize for the specific color
            price_info = next(
                (item for item in product.get("pricesBySize", []) if item.get("colour") == color),
                None
            )

            for size in sizes:
                if (size, sku) not in seen:
                    # Find price details for the specific size
                    size_price = None
                    if price_info:
                        size_price = next(
                            (sp for sp in price_info.get("sizePrices", []) if sp.get("size") == size),
                            None
                        )

                    # Extract price and compare price, converting to pounds if necessary
                    if size_price and "price" in size_price:
                        try:
                            raw_price = float(size_price["price"].get("centamount", 0))
                            raw_compare_price = float(size_price["price"].get("wasprice", 0))
                            # Divide by 100 if value is greater than 100 (assume cents)
                            price = raw_price / 100 if raw_price > 100 else raw_price
                            compare_price = raw_compare_price / 100 if raw_compare_price > 100 else raw_compare_price
                            #logging.debug(f"Product {handle}, Variant {sku}, Size {size}: "
                                       # f"centamount={raw_price}, wasprice={raw_compare_price}, "
                                        #f"Converted Price={price}, Converted Compare Price={compare_price}")
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Invalid price data for {handle}, size {size}: {e}")
                            price = 0.0
                            compare_price = 0.0
                    else:
                        # Fallback to variant price if pricesBySize is unavailable
                        raw_price = float(variant.get("price", [0])[0] if isinstance(variant.get("price"), list) else variant.get("price", 0))
                        raw_compare_price = float(variant.get("wasPrice", 0))
                        # Apply same conversion logic for fallback
                        price = raw_price / 100 if raw_price > 100 else raw_price
                        compare_price = raw_compare_price / 100 if raw_compare_price > 100 else raw_compare_price
                        # logging.debug(f"Using fallback for {handle}, Variant {sku}, Size {size}: "
                        #             f"Price={raw_price}, WasPrice={raw_compare_price}, "
                        #             f"Converted Price={price}, Converted Compare Price={compare_price}")

                    cleaned_products[handle]["variants"].append({
                        "Variant SKU": sku,
                        "size": size,
                        "color": color,
                        "Variant Price": price,
                        "Variant Compare At Price": compare_price,
                        "images": all_images
                    })
                    seen.add((size, sku))

    # Return as a list of product dicts
    return list(cleaned_products.values())

def fetch_and_clean_all_products(output_file, gender_tag=None, product_type=None, total_pages=50, max_workers=10):
    all_cleaned_products = {}

    def fetch_and_clean_page(page):
        logging.info(f"Fetching page {page + 1}/{total_pages}")
        data = fetch_products_from_algolia(page)
        if data:
            return clean_and_save_product_data_only_available_with_all_images_from_data(
                data, gender_tag, product_type
            )
        return []

    # Use ThreadPoolExecutor to fetch pages concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all page fetch tasks
        future_to_page = {executor.submit(fetch_and_clean_page, page): page for page in range(total_pages)}
        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            try:
                cleaned_data = future.result()
                # Merge products, ensuring no duplicates by handle
                for product in cleaned_data:
                    handle = product["Handle"]
                    if handle not in all_cleaned_products:
                        all_cleaned_products[handle] = product
                    else:
                        # Merge variants if product already exists
                        all_cleaned_products[handle]["variants"].extend(
                            variant for variant in product["variants"]
                            if variant["Variant SKU"] not in {v["Variant SKU"] for v in all_cleaned_products[handle]["variants"]}
                        )
            except Exception as e:
                logging.error(f"Page {page} generated an exception: {str(e)}")

    # Convert to list and save to file
    cleaned_products_list = list(all_cleaned_products.values())
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_products_list, f, indent=4, ensure_ascii=False)
        logging.info(f"Cleaned data has been saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save cleaned data: {str(e)}")

    return cleaned_products_list

def complete_workflow_boohoo():
    output_file = "cleaned_data.json"
    cleaned_data = fetch_and_clean_all_products(output_file, gender_tag="women", total_pages=50, max_workers=20)
    upsert_all_product_data(cleaned_data,BASE_URL,"GBP")

# Example usage
if __name__ == "__main__":
    complete_workflow_boohoo()