from supabase import create_client, Client
import json
from dotenv import load_dotenv
load_dotenv()
import os


    # Initialize Supabase client with service key
url = os.getenv("SUPABASE_URL")
service_key = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, service_key)


from datetime import datetime


def extract_and_update_colors_from_json(data):
    """
    Extract colors from data and update the colors row in the database.
    Useful for processing existing scraped data.
    """
    try:
        extracted_colors = extract_colors_from_products(data)
        print(f"🎨 Found {len(extracted_colors)} unique colors")
        
        if extracted_colors:
            print("🔄 Updating colors row in database...")
            update_colors_in_database(extracted_colors)
            return extracted_colors
        else:
            print("⚠️ No colors found to update")
            return []
            
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return []


def upsert_product(product_json: dict, website_url: str, currency: str):
    try:
        # Add website_url and type to the dict, but keep all other keys
        upsert_data = dict(product_json)
        upsert_data["website_url"] = website_url
        upsert_data["type"] = currency
        response = supabase.table("products").upsert(
            upsert_data,
            on_conflict="website_url"
        ).execute()
        print(f"Upsert successful for website_url '{website_url}'")
        return response
    except Exception as e:
        print(f"Error during upsert for website_url '{website_url}': {e}")
        return None

def extract_colors_from_products(products_data):
    """
    Extract unique colors from products data.
    Returns a list of color objects with mapped and original fields.
    """
    colors_set = set()
    
    if isinstance(products_data, dict) and "products" in products_data:
        products = products_data["products"]
    elif isinstance(products_data, list):
        products = products_data
    else:
        print("Invalid products data format")
        return []
    
    for product in products:
        if "variants" in product:
            for variant in product["variants"]:
                if "color" in variant and variant["color"]:
                    color = variant["color"].strip()
                    if color:  # Only add non-empty colors
                        colors_set.add(color)
    
    # Convert to the required format
    colors_list = [{"mapped": "", "original": color} for color in sorted(colors_set)]
    return colors_list

def update_colors_in_database(new_colors: list):
    """
    Update colors row in the database.
    Appends new colors without removing existing ones.
    """
    try:
        # First, get existing colors from the colors row
        existing_response = supabase.table("products").select("products").eq("website_url", "colors").execute()
        
        existing_colors = []
        if existing_response.data and existing_response.data[0].get("products"):
            # If products column contains JSON with "colors" key, extract the array
            products_data = existing_response.data[0]["products"]
            if isinstance(products_data, dict) and "colors" in products_data:
                existing_colors = products_data["colors"]
            elif isinstance(products_data, list):
                existing_colors = products_data
        
        # Create a set of existing original colors to avoid duplicates
        existing_original_colors = {color["original"] for color in existing_colors if isinstance(color, dict) and "original" in color}
        
        # Add only new colors that don't already exist
        colors_to_add = []
        for new_color in new_colors:
            if new_color["original"] not in existing_original_colors:
                colors_to_add.append(new_color)
                existing_original_colors.add(new_color["original"])
        
        # Combine existing and new colors
        all_colors = existing_colors + colors_to_add
        
        # Format as JSON with "colors" key
        colors_json = {"colors": all_colors}
        
        # Update the colors row in the database
        response = supabase.table("products").update(
            {"products": colors_json}
        ).eq("website_url", "colors").execute()
        
        print(f"✅ Colors row updated")
        print(f"📊 Total colors: {len(all_colors)} (Added {len(colors_to_add)} new colors)")
        return response
        
    except Exception as e:
        print(f"❌ Error updating colors row: {e}")
        return None

def get_product_by_website(website_url: str):
    """
    Retrieve a single product by website_url from the products table.
    """
    try:
        response = supabase.table("products").select("*").eq("website_url", website_url).execute()
        
        if response.data:
            #print("Product found:", json.dumps(response.data[0], indent=2))
            return response.data[0]
        else:
            print(f"No product found with website_url: {website_url}")
            return None
    except Exception as e:
        print(f"Error fetching product: {e}")
        return None

def upsert_tags_row(new_tags: list):
    """
    Upserts unique tags into the row where website_url = 'tags'.
    Appends new tags to existing ones, skipping duplicates.
    """
    try:
        # Fetch existing tags
        response = supabase.table("products").select("products").eq("website_url", "tags").execute()
        existing_tags = set()
        if response.data and response.data[0].get("products"):
            products_data = response.data[0]["products"]
            if isinstance(products_data, dict) and "tags" in products_data:
                existing_tags = set(products_data["tags"])
        # Combine and deduplicate
        combined_tags = sorted(existing_tags.union(set(new_tags)))
        tags_json = {"tags": combined_tags}
        supabase.table("products").upsert(
            {
                "products": tags_json,
                "website_url": "tags"
            },
            on_conflict="website_url"
        ).execute()
        print(f"✅ Tags upserted ({len(combined_tags)} total tags).")
    except Exception as e:
        print(f"Error upserting tags: {e}")

def upsert_all_product_data(cleaned_list, website_url, currency="USD"):
    """
    Upserts products, tags, and colors for a given website.
    """
    # Upsert products
    upsert_product(
        {
            "products": {"products": cleaned_list},
            "total_products": str(len(cleaned_list)),
            "updated_at": datetime.now().isoformat(sep=' ', timespec='seconds')
        },
        website_url,
        currency
    )
    print(f"✅ Saved {len(cleaned_list)} products for '{website_url}'")

    # Extract and upsert tags
    all_tags = set()
    for product in cleaned_list:
        tags_str = product.get("Tags", "")
        tags = [t.strip() for t in tags_str.split(",") if t.strip()]
        all_tags.update(tags)
    upsert_tags_row(list(all_tags))

    # Extract and upsert colors
    extracted_colors = extract_colors_from_products({"products": cleaned_list})
    if extracted_colors:
        print("🔄 Updating colors in database...")
        update_colors_in_database(extracted_colors)
    else:
        print("⚠️ No colors found to update")

# Example usage
if __name__ == "__main__":
    # Test the connection
    print("Testing Supabase connection:")

        # Perform an upsert
    print("\nUpserting product:")
    upsert_product(
        product_json={"product1": "Example Product", "price": 19.99},
        website_url="talal",
        currency="pound"
    )


    # Retrieve a specific product by website_url
    print("\nFetching product by website_url 'talal':")
    get_product_by_website("talal")