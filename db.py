from supabase import create_client, Client
import json

# Initialize Supabase client with service key
url = "https://aukawwdrhxjmibftigwf.supabase.co"
service_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF1a2F3d2RyaHhqbWliZnRpZ3dmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2MTA2NywiZXhwIjoyMDY2OTM3MDY3fQ.6r7-QZaQxPRQYpL5lgERsTcl3h6L1HJCzD4t8auXfK0"
supabase: Client = create_client(url, service_key)

def test_connection():
    """
    Test the Supabase connection by querying the products table.
    """
    try:
        response = supabase.table("products").select("count", count="exact").execute()
        print(f"Connection successful. Total rows in products table: {response.count}")
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

from datetime import datetime

def upsert_product(product_json: dict, website_url: str, currency: str):
    try:
        response = supabase.table("products").upsert(
            {
                "products": product_json,
                "website_url": website_url,
                "currency": currency
            },
            on_conflict=["website_url"]  # IMPORTANT: match conflict on this column
        ).execute()
        print(f"Upsert successful for website_url '{website_url}'")
        return response
    except Exception as e:
        print(f"Error during upsert for website_url '{website_url}': {e}")
        return None



def get_all_products():
    """
    Retrieve all products from the products table.
    """
    try:
        response = supabase.table("products").select("*").execute()
        
        if response.data:
            print("Products found:", json.dumps(response.data, indent=2))
            return response.data
        else:
            print("No products found in the table.")
            return []
    except Exception as e:
        print(f"Error fetching products: {e}")
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

# Example usage
if __name__ == "__main__":
    # Test the connection
    print("Testing Supabase connection:")
    if test_connection():
        # Perform an upsert
        print("\nUpserting product:")
        upsert_product(
            product_json={"product1": "Example Product", "price": 19.99},
            website_url="talal",
            currency="pound"
        )
        
        # Retrieve all products
        print("\nFetching all products:")
        get_all_products()
        
        # Retrieve a specific product by website_url
        print("\nFetching product by website_url 'talal':")
        get_product_by_website("talal")
    else:
        print("Cannot proceed due to connection failure.")

