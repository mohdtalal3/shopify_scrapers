import json
import pandas as pd
import os

def process_product_data(json_file="extracted_product_json.json"):
    """Process product data from extracted_product_json.json and convert to Shopify format"""
    # Load the JSON data with UTF-8 encoding
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    products = data["products"]
    rows = []
    
    for handle, product_data in products.items():
        base = product_data["base"]
        variants = product_data["variants"]
        images = product_data["images"]
        collections = product_data.get("collections", [])
        
        # Skip if no variants
        if not variants:
            continue
            
        # Determine if we have sizes
        has_size = any(v.get("size") for v in variants)
        
        # First row contains all product info
        is_first_row = True
        
        # Process each variant
        for idx, variant in enumerate(variants):
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
                row["Option1 Value"] = variant.get("size", "")
            else:
                row["Option1 Name"] = "Title"
                row["Option1 Value"] = base["Title"]
            
            # Add variant info
            row["Variant SKU"] = variant.get("sku", "")
            row["Variant Price"] = variant.get("price", 0)
            row["Variant Compare At Price"] = variant.get("compare_price", 0)
            row["Variant Inventory Qty"] = ""
            row["Variant Inventory Policy"] = variant.get("inventory_management", "deny")
            row["Variant Fulfillment Service"] = "manual"
            row["Variant Weight Unit"] = ""
            row["Variant Grams"] = ""
            
            # Default settings for variants
            if "Status" not in row:
                row["Status"] = "active"
            if "Published" not in row:
                row["Published"] = "TRUE"
            
            # Add the first image to the main variant row
            if images:
                first_img_key = list(images.keys())[0]
                first_img_url = images[first_img_key][0]
                row["Image Src"] = first_img_url
                row["Image Position"] = int(first_img_key)
            
            # Add the main variant row
            rows.append(row)
            
            # Add additional images as separate rows for this variant
            if images:
                # Add remaining images from the first position (if any)
                first_img_key = list(images.keys())[0]
                if len(images[first_img_key]) > 1:
                    for img_url in images[first_img_key][1:]:
                        img_row = {
                            "Handle": base["Handle"],
                            "Option1 Name": row.get("Option1 Name", ""),
                            "Option1 Value": row.get("Option1 Value", ""),
                            "Variant SKU": variant.get("sku", ""),
                            "Variant Inventory Policy": variant.get("inventory_management", "deny"),
                            "Variant Fulfillment Service": "manual",
                            "Variant Price": variant.get("price", 0),
                            "Variant Compare At Price": variant.get("compare_price", 0),
                            "Image Src": img_url, 
                            "Image Position": int(first_img_key),
                            "Status": "active"
                        }
                        rows.append(img_row)
                
                # Add all images from other positions
                for img_position, img_urls in images.items():
                    if int(img_position) != int(first_img_key):
                        for img_url in img_urls:
                            img_row = {
                                "Handle": base["Handle"],
                                "Option1 Name": row.get("Option1 Name", ""),
                                "Option1 Value": row.get("Option1 Value", ""),
                                "Variant SKU": variant.get("sku", ""),
                                "Variant Inventory Policy": variant.get("inventory_management", "deny"),
                                "Variant Fulfillment Service": "manual",
                                "Variant Price": variant.get("price", 0),
                                "Variant Compare At Price": variant.get("compare_price", 0),
                                "Image Src": img_url, 
                                "Image Position": int(img_position),
                                "Status": "active"
                            }
                            rows.append(img_row)
    
    return rows


def create_shopify_csv(rows, output_file="shopify_products.csv"):
    """Create Shopify-compatible CSV file, appending to existing data if file exists"""
    # Create DataFrame from new rows
    new_df = pd.DataFrame(rows)
    
    # Define Shopify columns
    shopify_columns = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags", "Published",
        "Option1 Name", "Option1 Value",
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
        # Read existing data with UTF-8 encoding
        existing_df = pd.read_csv(output_file, encoding="utf-8")
        
        # Concatenate with new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on Handle and Variant SKU
        combined_df = combined_df.drop_duplicates(subset=["Handle", "Variant SKU", "Image Position"], keep="last")
        
        # Save combined data with UTF-8 encoding
        combined_df.to_csv(output_file, index=False, encoding="utf-8")
        print(f"[✓] Appended new data to existing Shopify CSV file: {output_file}")
    else:
        # If file doesn't exist, create new one with UTF-8 encoding
        new_df.to_csv(output_file, index=False, encoding="utf-8")
        print(f"[✓] Created new Shopify CSV file: {output_file}")
    
    return new_df


def main():
    """Main function to run the conversion process"""
    try:
        print("Starting conversion from extracted_product_json.json to Shopify CSV format...")
        
        # Process the product data
        rows = process_product_data("extracted_product_json.json")
        
        if not rows:
            print("No product data found to convert.")
            return
        
        print(f"Processed {len(rows)} rows of product data.")
        
        # Create the CSV file
        df = create_shopify_csv(rows, "shopify_products.csv")
        
        print(f"Conversion completed successfully!")
        print(f"Total products processed: {len(df['Handle'].unique())}")
        print(f"Total variants processed: {len(df)}")
        
    except FileNotFoundError:
        print("Error: extracted_product_json.json file not found in the current directory.")
    except Exception as e:
        print(f"Error during conversion: {str(e)}")


if __name__ == "__main__":
    main()