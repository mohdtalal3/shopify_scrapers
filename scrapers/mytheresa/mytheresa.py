import json
import re
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from db import *
import requests
import time
BASE_URL = "https://www.mytheresa.com"


def fetch_mytheresa_data():
    # URL endpoint
    url = "https://api.mytheresa.com/api"

    # Headers
    headers = {
        "Accept-Language": "en",  # ✅ FIXED HERE
        "Content-Type": "text/plain;charset=UTF-8",  # or try application/json
        "Accept": "*/*",
        "Origin": "https://www.mytheresa.com",
        "Referer": "https://www.mytheresa.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "X-Country": "US",         # ✅ This matters (try DE, GB, US)
        "X-Nsu": "false",
        "X-Op": "ntr",
        "X-Region": "BY",
        "X-Section": "men",      # ✅ Use "men", "women", or "kids"
        "X-Store": "US",           # ✅ Important! Use "DE", "en-gb", "en-us", etc.
    }


    # Full payload as provided (query + variables)

    query= """query XProductListingPageQuery($categories: [String], $colors: [String], $designers: [String], $fta: Boolean, $materials: [String], $page: Int, $patterns: [String], $reductionRange: [String], $saleStatus: SaleStatusEnum, $size: Int, $sizesHarmonized: [String], $slug: String, $sort: String) {
        xProductListingPage(categories: $categories, colors: $colors, designers: $designers, fta: $fta, materials: $materials, page: $page, patterns: $patterns, reductionRange: $reductionRange, saleStatus: $saleStatus, size: $size, sizesHarmonized: $sizesHarmonized, slug: $slug, sort: $sort) {
        id
        alternateUrls {
            language
            store
            url
        }
        breadcrumb {
            id
            name
            slug
        }
        combinedDepartmentGroupAndCategoryErpID
        department
        designerErpId
        displayName
        facets {
            categories {
            name
            options {
                id
                name
                slug
                children {
                id
                name
                slug
                children {
                    id
                    name
                    slug
                }
                }
            }
            activeValue
            }
            designers {
            name
            options {
                value
                slug
            }
            activeValue
            }
            colors {
            name
            options {
                value
            }
            activeValue
            }
            fta {
            activeValue
            name
            options {
                value
            }
            visibility
            }
            materials {
            activeValue
            name
            options {
                value
            }
            visibility
            }
            patterns {
            name
            options {
                value
            }
            activeValue
            }
            reductionRange {
            activeValue
            name
            options {
                value
            }
            unit
            visibility
            }
            saleStatus {
            activeValue
            name
            options {
                value
            }
            visibility
            }
            sizesHarmonized {
            name
            options {
                value
            }
            activeValue
            }
        }
        isMonetisationExcluded
        isSalePage
        pagination {
            currentPage
            itemsPerPage
            totalItems
            totalPages
        }
        products {
            color
            combinedCategoryErpID
            combinedCategoryName
            department
            description
            designer
            designerErpId
            designerInfo {
            designerId
            displayName
            slug
            }
            displayImages
            enabled
            features
            fta
            hasMultipleSizes
            hasSizeChart
            hasStock
            isComingSoon
            isInWishlist
            isPurchasable
            isSizeRelevant
            labelObjects {
            id
            label
            }
            labels
            mainPrice
            mainWaregroup
            name
            price {
            currencyCode
            currencySymbol
            discount
            discountEur
            extraDiscount
            finalDuties
            hint
            includesVAT
            isPriceModifiedByRegionalRules
            original
            originalDuties
            originalDutiesEur
            originalEur
            percentage
            regionalRulesModifications {
                priceColor
            }
            regular
            vatPercentage
            }
            priceDescription
            promotionLabels {
            label
            type
            }
            seasonCode
            sellerOrigin
            sets
            sizeAndFit
            sizesOnStock
            sizeTag
            sizeType
            sku
            slug
            variants {
            allVariants {
                availability {
                hasStock
                lastStockQuantityHint
                }
                isInWishlist
                size
                sizeHarmonized
                sku
            }
            availability {
                hasStock
                lastStockQuantityHint
            }
            isInWishlist
            price {
                currencyCode
                currencySymbol
                discount
                discountEur
                extraDiscount
                includesVAT
                isPriceModifiedByRegionalRules
                original
                originalEur
                percentage
                regionalRulesModifications {
                priceColor
                }
                vatPercentage
            }
            size
            sizeHarmonized
            sku
            }
        }
        sort {
            currentParam
            params
        }
        }
    }"""

    # Base variables for the query
    base_variables = {
        "categories": [],
        "colors": [],
        "designers": ["Ami Paris", "Amiri", "Balenciaga", "Burberry", "Dolce&Gabbana", "Fendi", "Givenchy", "Golden Goose", "Gucci", "Kenzo", "Lanvin", "Loewe", "On", "Polo Ralph Lauren", "Prada", "Saint Laurent", "Ralph Lauren Purple Label", "Rick Owens", "Stone Island", "The Row", "Tom Ford", "Tod's", "Valentino Garavani", "Valentino", "Versace"],
        "fta": None,
        "materials": [],
        "page": 1,
        "patterns": [],
        "reductionRange": [],
        "saleStatus": None,
        "size": 120,
        "sizesHarmonized": [],
        "slug": "/sale/previous-season",
        "sort": "price_desc"
    }
    # Collect all data here
    all_products = []

    def fetch_page(page_num):
        variables = base_variables.copy()
        variables["page"] = page_num
        payload = {
            "query": query,
            "variables": variables
        }
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()

    # Fetch first page to determine total pages
    first_response = fetch_page(1)
    all_products.append(first_response)

    # Determine total pages from pagination
    pagination = first_response.get("data", {}).get("xProductListingPage", {}).get("pagination", {})
    total_pages = pagination.get("totalPages", 1)

    print(f"Total pages: {total_pages}")

    # Loop through remaining pages
    for page in range(2, total_pages + 1):
        print(f"Fetching page {page}...")
        try:
            page_data = fetch_page(page)
            all_products.append(page_data)
            time.sleep(1)  # Respectful delay
        except Exception as e:
            print(f"Error fetching page {page}: {e}")


    return all_products



# Function to clean and transform product data
def clean_mytheresa_data(data, gender_tag=None):
    products = []
    for page_data in data:
        page_products = page_data.get("data", {}).get("xProductListingPage", {}).get("products", [])
        if isinstance(page_products, list):
            products.extend(page_products)

    cleaned_products = {}

    for product in products:
        if not product.get("hasStock", True):
            continue

        handle = product.get("slug")
        title = product.get("name")
        description = product.get("description", "")
        brand = product.get("designer", "")
        product_tags = []

        combined_category = product.get("combinedCategoryName", "")
        split_categories = combined_category.split("::")
        product_type = split_categories[-1] if split_categories else ""
        category_val = gender_tag.lower() if gender_tag else ""
        tags = list(set(split_categories))  # Extract from combinedCategoryName
        waregroup = product.get("mainWaregroup", "")
        gender_tags = set()
        if gender_tag:
            if gender_tag.lower() == "men" and "clothing" in waregroup:
                gender_tags = {"all clothing men", "mens", "men clothing", "men"}
            elif gender_tag.lower() == "women" and "clothing" in waregroup:
                gender_tags = {"all clothing women", "womens", "women clothing", "women"}
            else:
                gender_tags = {"men", "women", "unisex", "shoes", "unisex"}

        all_tags = list(set(tags + product_tags + list(gender_tags)))
        tags_str = ', '.join(sorted(all_tags))

        all_images = list(set(product.get("displayImages", [])))

        if handle not in cleaned_products:
            cleaned_products[handle] = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": f"<p>{description}</p>",
                "Vendor": brand,
                "Product Category": category_val,
                "Type": product_type,
                "Tags": tags_str,
                "variants": []
            }

        seen = set()
        for variant in product.get("variants", []):
            if not variant.get("availability", {}).get("hasStock", False):
                continue

            sku = variant.get("sku", "")
            price = float(variant.get("price", {}).get("discount", 0))
            compare_price = float(variant.get("price", {}).get("original", 0))
            size = variant.get("size")
            color = product.get("color", "")

            if (size, sku) not in seen:
                cleaned_products[handle]["variants"].append({
                    "Variant SKU": sku,
                    "size": size,
                    "color": color,
                    "Variant Price": int(price / 100) if price % 100 == 0 else round(price / 100, 2),
                    "Variant Compare At Price": int(compare_price / 100) if compare_price % 100 == 0 else round(compare_price / 100, 2),
                    "images": all_images
                })
                seen.add((size, sku))
    # with open("mytheresa_data_cleaned.json", "w", encoding="utf-8") as f:
    #     json.dump(cleaned_products, f, indent=2, ensure_ascii=False)
    # print("✅ Data saved to 'mytheresa_data_cleaned.json'")
    return list(cleaned_products.values())


def complete_workflow_mytheresa():
    data = fetch_mytheresa_data()
    cleaned_output = clean_mytheresa_data(data, gender_tag="men")
    upsert_all_product_data(cleaned_output, BASE_URL, "USD")


if __name__ == "__main__":
    data = fetch_mytheresa_data()
    cleaned_output = clean_mytheresa_data(data, gender_tag="men")
    upsert_all_product_data(cleaned_output, BASE_URL, "USD")

