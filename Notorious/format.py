def process_product_data(json_file="response_data.json"):
    """Process product data from JSON file and convert to Shopify format"""
    # Load the JSON data
    with open(json_file, "r") as f:
        data = json.load(f)

    products = data["data"]["products"]
    structured_products = {}

    # First pass - organize products by handle
    for product in products:
        handle = product["attributes"]["url"].split("#")[0]
        
        if handle not in structured_products:
            # Create base product data
            structured_products[handle] = {
                "base": {
                    "Handle": handle,
                    "Title": product["name"],
                    "Body (HTML)": f"<p>{product['description']}</p>".replace("\r\n", "<br>"),
                    "Vendor": product["attributes"]["brand"],
                    "Product Category": f"{product['attributes'].get('category', '')} > {product['attributes'].get('gender', '')} > {product['attributes'].get('activityGroup', '')}",
                    "Type": product["attributes"]["subCategory"],
                    "Tags": f"{product['attributes'].get('gender', '')}, {product['attributes'].get('activityGroup', '')}, {product['attributes'].get('color', '')}",
                    "Published": "TRUE",
                    "Status": "active",
                    "Variant Inventory Policy": "deny",
                    "Variant Fulfillment Service": "manual"
                },
                "variants": [],
                "images": {}
            }
        
        # Color from product attributes
        color = product["attributes"].get("color", "")
        
        # Add variants
        for variant in product["variants"]:
            sku = variant.get("sku", "")
            size = variant.get("size", "")
            price = variant.get("price", {}).get("value", {}).get("centAmount", 0) / 100
            compare_price = variant.get("ticketPrice", {}).get("value", {}).get("centAmount", 0) / 100
            
            variant_data = {
                "sku": sku,
                "size": size,
                "color": color,
                "price": price,
                "compare_price": compare_price
            }
            
            structured_products[handle]["variants"].append(variant_data)
            
            # Collect images for this variant
            if sku not in structured_products[handle]["images"]:
                structured_products[handle]["images"][sku] = []
                
            for image in variant.get("images", []):
                structured_products[handle]["images"][sku].append(image["url"])

    # Second pass - generate rows for Shopify
    rows = []
    
    for handle, product_data in structured_products.items():
        base = product_data["base"]
        variants = product_data["variants"]
        
        # Skip if no variants
        if not variants:
            continue
            
        # Determine if we have sizes
        has_size = any(v["size"] for v in variants)
        
        # Group variants by color
        variants_by_color = {}
        for variant in variants:
            color = variant["color"]
            if color not in variants_by_color:
                variants_by_color[color] = []
            variants_by_color[color].append(variant)
        
        # First row contains all product info
        is_first_row = True
        
        # Process each color
        for color, color_variants in variants_by_color.items():
            # Skip if no variants for this color
            if not color_variants:
                continue
                
            # Process each size in this color
            for idx, variant in enumerate(color_variants):
                row = {}
                
                # For first row, include all product info
                if is_first_row:
                    row = base.copy()
                    is_first_row = False
                else:
                    # Only include Handle for subsequent rows
                    row["Handle"] = base["Handle"]
                
                # Set options based on whether we have size
                if has_size:
                    row["Option1 Name"] = "Size"
                    row["Option1 Value"] = variant["size"]
                    row["Option2 Name"] = "Color"
                    row["Option2 Value"] = color
                else:
                    row["Option1 Name"] = "Title"
                    row["Option1 Value"] = base["Title"]
                    row["Option2 Name"] = "Color"
                    row["Option2 Value"] = color
                
                # Add variant info
                row["Variant SKU"] = variant["sku"]
                row["Variant Price"] = variant["price"]
                row["Variant Compare At Price"] = variant["compare_price"]
                
                # Default settings for variants
                if "Variant Inventory Policy" not in row:
                    row["Variant Inventory Policy"] = "deny"
                if "Variant Fulfillment Service" not in row:
                    row["Variant Fulfillment Service"] = "manual"
                if "Status" not in row:
                    row["Status"] = "active"
                    
                # Get images for this variant
                images = product_data["images"].get(variant["sku"], [])
                
                # If no images found for this specific SKU, use first variant images as fallback
                if not images and product_data["images"]:
                    first_sku = list(product_data["images"].keys())[0]
                    images = product_data["images"][first_sku]
                
                # Add first image to the variant row
                if images:
                    row["Image Src"] = images[0]
                    row["Image Position"] = 1
                    rows.append(row)
                    
                    # Add additional images as separate rows
                    for img_idx, img_url in enumerate(images[1:], 2):
                        img_row = {"Handle": base["Handle"], "Image Src": img_url, "Image Position": img_idx}
                        rows.append(img_row)
                else:
                    # Even with no images, still add the variant
                    rows.append(row)
    
    return rows


def create_shopify_csv(rows, output_file="shopify_products.csv"):
    """Create Shopify-compatible CSV file, appending to existing data if file exists"""
    # Create DataFrame from new rows
    new_df = pd.DataFrame(rows)
    
    # Define Shopify columns
    shopify_columns = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags", "Published",
        "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value",
        "Variant SKU", "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
        "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price", "Variant Requires Shipping",
        "Variant Taxable", "Variant Barcode", "Image Src", "Image Position", "Image Alt Text", "Gift Card",
        "SEO Title", "SEO Description", "Google Shopping / Google Product Category", "Google Shopping / Gender",
        "Google Shopping / Age Group", "Google Shopping / MPN", "Google Shopping / Condition",
        "Google Shopping / Custom Product", "Variant Image", "Variant Weight Unit", "Variant Tax Code",
        "Cost per item", "Included / United States", "Price / United States", "Compare At Price / United States",
        "Included / International", "Price / International", "Compare At Price / International", "Status"
    ]
    
    # Add missing columns to new data with empty values
    for col in shopify_columns:
        if col not in new_df.columns:
            new_df[col] = ""
    
    # Order columns in new data
    new_df = new_df[shopify_columns]
    
    # Check if file exists, if so read and append
    if os.path.exists(output_file):
        # Read existing data
        existing_df = pd.read_csv(output_file)
        
        # Concatenate with new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on Handle and Variant SKU
        combined_df = combined_df.drop_duplicates(subset=["Handle", "Variant SKU", "Image Position"], keep="last")
        
        # Save combined data
        combined_df.to_csv(output_file, index=False)
        print(f"[✓] Appended new data to existing Shopify CSV file: {output_file}")
    else:
        # If file doesn't exist, create new one
        new_df.to_csv(output_file, index=False)
        print(f"[✓] Created new Shopify CSV file: {output_file}")
    
    return new_df