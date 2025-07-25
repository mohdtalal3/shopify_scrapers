import requests
import json
import time
import json
import re
from typing import List, Dict, Any
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import *
from dotenv import load_dotenv
load_dotenv()
import concurrent.futures
proxy_str = os.getenv("PROXY_URL")

# Proxies dictionary for requests
proxies = {
    "http": proxy_str,
    "https": proxy_str
}

print(proxies)
BASE_URL = "https://www.lululemon.com"
def clean_lululemon_data(products: List[Dict[str, Any]], gender_tag: str = None) -> List[Dict[str, Any]]:
    cleaned_products = {}

    for product_data in products:
        # Extract product details from the nested structure
        product = product_data.get("data", {}).get("data", {}).get("productDetailPage", {})
        if not product:
            continue

        # Use pdpUrl as handle (remove leading slash), fallback to productId
        raw_url = product.get("productSummary", {}).get("pdpUrl", "").strip()
        handle = raw_url.strip("/") if raw_url else product.get("productSummary", {}).get("productId", "")

        if not handle:
            continue  # Skip if no handle can be found

        title = product.get("productSummary", {}).get("displayName", "")
        brand = "Lululemon"
        
        # Extract first colorAttributes for description
        color_attrs = product.get("colorAttributes", [])
        description = f"<p>{brand} - {title}</p>"
        if color_attrs and len(color_attrs) > 0:
            first_attr = color_attrs[0]
            wwmt = first_attr.get("wwmt", "")
            fabric_benefits = first_attr.get("fabricOrBenefits", {}).get("title", "") + ": " + ", ".join(
                attr.get("text", "") for section in first_attr.get("fabricOrBenefits", {}).get("sections", [])
                for attr in section.get("attributes", []) if attr.get("text")
            )
            features = first_attr.get("featuresOrIngredients", {}).get("title", "") + ": " + ", ".join(
                attr.get("text", "") for section in first_attr.get("featuresOrIngredients", {}).get("sections", [])
                for attr in section.get("attributes", []) if attr.get("text")
            )
            description += f"<p>{wwmt}</p>" if wwmt else ""
            description += f"<p>{fabric_benefits}</p>" if fabric_benefits else ""
            description += f"<p>{features}</p>" if features else ""
        
        # Category and type
        category_val = product.get("category", {}).get("name", "")
        type_val = product.get("productSummary", {}).get("type", "")

        # Gender tags
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men":
                gender_tags = {"mens", "men"}
            elif gender_tag.lower() == "women":
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}
            else:
                gender_tags = {"men", "women", "unisex", "shoes"}

        cat_main = category_val
        cat_part1 = ""
        cat_part2 = ""

        if "&" in category_val:
            parts = [p.strip() for p in category_val.split("&", 1)]
            cat_part1, cat_part2 = parts if len(parts) == 2 else (parts[0], "")
            # Optionally overwrite cat_main with first part
            cat_main = cat_part1

        # Build product tags, including split parts if present
        product_tags = [
            cat_main,
            cat_part1,
            cat_part2,
            type_val,
            *product.get("productSummary", {}).get("activity", [])
        ]

        # Merge tags and make unique comma-separated string
        all_tags = product_tags + list(gender_tags)
        tags_str = ', '.join(sorted(set(filter(None, all_tags))))

        # Create a mapping of color names to their images
        color_to_images = {}
        for carousel in product.get("productCarousel", []):
            color_name = carousel.get("color", {}).get("name", "").lower()
            if color_name:
                images = carousel.get("imageInfo", [])
                if images:
                    color_to_images[color_name] = list(set(images))  # Remove duplicates within color

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": brand,
                "Product Category": category_val.lower(),
                "Type": re.sub(r'\bwomens?\b', '', type_val, flags=re.IGNORECASE).strip(),
                "Tags": tags_str,
                "variants": []
            }

        seen = set()
        for sku in product.get("skus", []):
            # Only process variants that are available in stock
            if not sku.get("available", False):
                continue

            variant_sku = sku.get("id", "")
            size = sku.get("size", "")
            color = sku.get("color", {}).get("name", "").lower()
            list_price = float(sku.get("price", {}).get("listPrice", "0"))
            sale_price = float(sku.get("price", {}).get("salePrice", "0")) if sku.get("price", {}).get("onSale", False) else None
            variant_price = sale_price if sale_price is not None else list_price
            compare_at_price = list_price

            # Get images for this specific color
            variant_images = color_to_images.get(color, [])

            if (size, variant_sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": variant_sku,
                    "size": size,
                    "color": color.capitalize(),
                    "Variant Price": variant_price,
                    "Variant Compare At Price": compare_at_price,
                    "images": variant_images
                })
                seen.add((size, variant_sku))

        # Remove products with no available variants
        if not cleaned_products[handle]["variants"]:
            del cleaned_products[handle]

    return list(cleaned_products.values())
def product_data(product_ids):
    # GraphQL endpoint
    graphql_url = "https://shop.lululemon.com/cne/graphql"
    # GraphQL query
    query = """
    query GetPdpDataById(
      $id: String!
      $category: String!
      $unifiedId: String!
      $locale: String
      $forceMemberCheck: Boolean
      $sl: String
      $forcePcm: Boolean
    ) {
      productDetailPage(
        id: $id
        category: $category
        unifiedId: $unifiedId
        locale: $locale
        forceMemberCheck: $forceMemberCheck
        sl: $sl
        forcePcm: $forcePcm
      ) {
        productSummary {
          pdpUrl
          productId
          displayName
          type
          activity
        }
        category {
          name
        }
        colorAttributes {
          wwmt
          fabricOrBenefits {
            sections {
              attributes {
                text
              }
            }
          }
          featuresOrIngredients {
            sections {
              attributes {
                text
              }
            }
          }
        }
        productCarousel {
          color {
            name
          }
          imageInfo
        }
        skus {
          id
          available
          size
          color {
            name
          }
          price {
            listPrice
            onSale
            salePrice
          }
        }
      }
    }

    """

    # Shared variables template
    base_variables = {
        "category": "",
        "unifiedId": "",
        "locale": "en-us",
        "forceMemberCheck": False,
        "sl": None,
        "forcePcm": True,
        "fetchPcmMedia": False,
        "fetchVariants": False,
        "fetchHighlights": False
    }
    proxy_str = os.getenv("PROXY_URL")

    # Proxies dictionary for requests
    proxies = {
        "http": proxy_str,
        "https": proxy_str
    }
    
    # Headers
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }

    def fetch_single_product(pid):
        try:
            variables = base_variables.copy()
            variables["id"] = pid
            payload = {
                "query": query,
                "variables": variables
            }
            # 1. First check your IP using the same proxy
            # try:
            #     ip_check = requests.get("https://api.ipify.org?format=json", proxies=proxies, timeout=10)
            #     print("Current IP:", ip_check.json()["ip"])
            # except Exception as e:
            #     print("Failed to check IP:", e)
            # input("Press Enter to continue...")
            
            response = requests.post(graphql_url, json=payload, headers=headers, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                print(f"Fetched data for {pid}")
                return {"id": pid, "data": data}
            else:
                print(f"Error fetching {pid}: {response.status_code}")
                print(response.text)
                return None
        except Exception as e:
            print(f"Exception occurred for {pid}: {e}")
            return None

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pid = {executor.submit(fetch_single_product, pid): pid for pid in product_ids}
        for future in concurrent.futures.as_completed(future_to_pid):
            result = future.result()
            if result is not None:
                all_results.append(result)

    return all_results

def get_product_ids():
    url = "https://shop.lululemon.com/snb/graphql"
    query = """query CategoryPageDataQuery(
      $category: String!
      $cid: String
      $forceMemberCheck: Boolean
      $nValue: String
      $cdpHash: String
      $sl: String!
      $locale: String!
      $Ns: String
      $storeId: String
      $pageSize: Int
      $page: Int
      $onlyStore: Boolean
      $useHighlights: Boolean
      $abFlags: [String]
      $styleboost: [String]
      $fusionExperimentVariant: String
    ) {
      categoryPageData(
        category: $category
        nValue: $nValue
        cdpHash: $cdpHash
        locale: $locale
        sl: $sl
        Ns: $Ns
        page: $page
        pageSize: $pageSize
        storeId: $storeId
        onlyStore: $onlyStore
        forceMemberCheck: $forceMemberCheck
        cid: $cid
        useHighlights: $useHighlights
        abFlags: $abFlags
        styleboost: $styleboost
        fusionExperimentVariant: $fusionExperimentVariant
      ) {
        totalProductPages
        products {
          productId
        }
      }
    }"""

    variables = {
        "pageSize": 350,
        "page": 1,
        "useHighlights": True,
        "onlyStore": False,
        "abFlags": ["cdpSeodsEnabled"],
        "category": "women-clothes",
        "cdpHash": "n10hryz2my0z2rruz3197z4uwkz8182zcld7zdeqfze8njzg62mzgo1xzlgk3zlja1zmnkcznibpzo05wzo1ewzoh18zpyjdzq46hzsgwgzsklfzstp1ztfe7zu1jrzug19zvmgwzvx03zwbwnzwo6vzxc7r",
        "forceMemberCheck": False,
        "fusionExperimentVariant": "",
        "locale": "en_US",
        "Ns": "price|1",
        "nValue": None,
        "sl": "US",
        "storeId": None,
        "styleboost": []
    }

    headers = {
        "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }

    product_ids = []
    total_pages = None

    for page_num in range(1, 1000):
        variables["page"] = page_num
        try:
            response = requests.post(url, headers=headers, json={"query": query, "variables": variables},proxies=proxies)
            response.raise_for_status()
            data = response.json()
            page_data = data["data"]["categoryPageData"]

            if total_pages is None:
                total_pages = page_data["totalProductPages"]
                print(total_pages)
            products = page_data.get("products", [])
            product_ids.extend([p["productId"] for p in products])
            #break
            if page_num >= total_pages:
                break

            time.sleep(0.3)
        except Exception as e:
            print(f"Error on page {page_num}: {e}")
            break
    return product_ids






def complete_workflow_lululemon():
    product_ids = get_product_ids()
    data = product_data(product_ids)
    cleaned_data = clean_lululemon_data(data, gender_tag="women")
    # with open("lululemon_cleaned_data.json", "w", encoding="utf-8") as f:
    #     json.dump(cleaned_data, f, indent=2)
    upsert_all_product_data(cleaned_data, BASE_URL, currency="USD")
if __name__ == "__main__":
    complete_workflow_lululemon()