import json
import pandas as pd
import os

def json_to_shopify_csv(cleaned_json_file="cleaned_products.json", output_file="shopify_products.csv"):
    """Read cleaned JSON and create Shopify CSV."""
    with open(cleaned_json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    products = data.get("products", [])

    rows = []
    handle_first_row = {}
    for product in products:
        base = {
            "Handle": product["Handle"],
            "Title": product["Title"],
            "Body (HTML)": product["Body (HTML)"],
            "Vendor": product["Vendor"],
            "Product Category": product["Product Category"],
            "Type": product["Type"],
            "Tags": product["Tags"],
            "Published": "TRUE",
            "Status": "active",
            "Variant Inventory Policy": "deny",
            "Variant Fulfillment Service": "manual"
        }
        variants = product["variants"]
        handle = product["Handle"]
        handle_first_row[handle] = True
        for variant in variants:
            color = variant.get("color")
            size = variant.get("size")
            color_val = color if color not in [None, "", "null"] else None
            size_val = size if size not in [None, "", "null"] else None
            # Option logic
            if color_val and size_val:
                opt1_name, opt1_val = "Size", size_val
                opt2_name, opt2_val = "Color", color_val
            elif color_val and not size_val:
                opt1_name, opt1_val = "Color", color_val
                opt2_name, opt2_val = "", ""
            elif size_val and not color_val:
                opt1_name, opt1_val = "Size", size_val
                opt2_name, opt2_val = "", ""
            else:
                opt1_name, opt1_val = "Title", product["Title"]
                opt2_name, opt2_val = "", ""
            images = variant.get("images", [])
            # First row: all info
            row = base.copy()
            if handle_first_row[handle]:
                row["Option1 Name"] = opt1_name
                row["Option2 Name"] = opt2_name
            else:
                row["Option1 Name"] = ""
                row["Option2 Name"] = ""
            row["Option1 Value"] = opt1_val
            row["Option2 Value"] = opt2_val
            row["Variant SKU"] = variant["Variant SKU"]
            row["Variant Price"] = variant["Variant Price"]
            row["Variant Compare At Price"] = variant["Variant Compare At Price"]
            row["Variant Inventory Policy"] = base["Variant Inventory Policy"]
            row["Variant Fulfillment Service"] = base["Variant Fulfillment Service"]
            row["Status"] = base["Status"]
            row["Image Src"] = images[0] if images else ""
            row["Image Position"] = 1 if images else ""
            # Only fill these columns for the first row of each handle
            if handle_first_row[handle]:
                handle_first_row[handle] = False
            else:
                for col in ["Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags", "Published"]:
                    row[col] = ""
            rows.append(row)
            # Additional image rows: repeat only required fields
            for img_idx, img_url in enumerate(images[1:], 2):
                img_row = {
                    "Handle": base["Handle"],
                    "Option1 Name": "",
                    "Option1 Value": opt1_val,
                    "Option2 Name": "",
                    "Option2 Value": opt2_val,
                    "Variant Price": variant["Variant Price"],
                    "Variant Compare At Price": variant["Variant Compare At Price"],
                    "Variant Inventory Policy": base["Variant Inventory Policy"],
                    "Variant Fulfillment Service": base["Variant Fulfillment Service"],
                    "Status": base["Status"],
                    "Image Src": img_url,
                    "Image Position": img_idx,
                }
                rows.append(img_row)

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
    new_df = pd.DataFrame(rows)
    for col in shopify_columns:
        if col not in new_df.columns:
            new_df[col] = ""
    new_df = new_df[shopify_columns]
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["Handle", "Variant SKU", "Image Position"], keep="last")
        combined_df.to_csv(output_file, index=False)
        print(f"[✓] Appended new data to existing Shopify CSV file: {output_file}")
    else:
        new_df.to_csv(output_file, index=False)
        print(f"[✓] Created new Shopify CSV file: {output_file}")

if __name__ == "__main__":
    json_to_shopify_csv() 