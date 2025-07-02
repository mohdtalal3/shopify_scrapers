from supabase import create_client, Client
import json
# Initialize Supabase client
url = "https://aukawwdrhxjmibftigwf.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF1a2F3d2RyaHhqbWliZnRpZ3dmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTEzNjEwNjcsImV4cCI6MjA2NjkzNzA2N30.C1Rbwg266fau-CpdY4KhqXL5sIb78-NpXfY0Ea620Bc"
supabase: Client = create_client(url, key)

def upsert_product(product_json: dict, website_url: str):
    # Construct raw SQL query using Supabase RPC or SQL execution
    query = f"""
    INSERT INTO public.products (products, website_url)
    VALUES (%s, %s)
    ON CONFLICT (website_url)
    DO UPDATE SET
      products = EXCLUDED.products,
      updated_at = NOW();
    """

    try:
        # Use Supabase's built-in SQL execution via `rpc` or `postgrest` SQL
        data = supabase.rpc("execute_sql", {
            "sql": query,
            "params": [json.dumps(product_json), website_url]
        })
        print("Upsert successful:", data)
    except Exception as e:
        print("Error during upsert:", e)


def get_product_by_website(website_url: str):
    try:
        response = supabase.table("products") \
            .select("*") \
            .eq("website_url", website_url) \
            .execute()
        
        if response.data:
            print("Product found:", response.data[0])
            return response.data[0]
        else:
            print("No product found with that URL.")
            return None
    except Exception as e:
        print("Error fetching product:", e)
        return None
# upsert_product(
#     product_json={"name": "Sample Product", "price": 19.99, "in_stock": True},
#     website_url="https://example.com/product/123"
# )


get_product_by_website("talal")