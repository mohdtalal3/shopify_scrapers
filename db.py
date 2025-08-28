import os
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Direct Postgres connection string (port 5432)
DATABASE_URL = os.getenv("DATABASE_URL")
# -------------------------------------------------------
# Helper: create connection and extend timeout
# -------------------------------------------------------
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '10min'")
    return conn, cur

# -------------------------------------------------------
# COLORS
# -------------------------------------------------------
def extract_colors_from_products(products_data):
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
                    if color:
                        colors_set.add(color)

    colors_list = [{"mapped": "", "original": color} for color in sorted(colors_set)]
    return colors_list

def update_colors_in_database(new_colors: list):
    try:
        conn, cur = get_connection()

        # 1. Fetch existing colors
        cur.execute("SELECT products FROM products WHERE website_url = %s", ("colors",))
        row = cur.fetchone()
        existing_colors = []
        if row and row[0]:
            products_data = row[0]
            if isinstance(products_data, dict) and "colors" in products_data:
                existing_colors = products_data["colors"]
            elif isinstance(products_data, list):
                existing_colors = products_data

        existing_original_colors = {c["original"] for c in existing_colors if isinstance(c, dict) and "original" in c}

        colors_to_add = []
        for new_color in new_colors:
            if new_color["original"] not in existing_original_colors:
                colors_to_add.append(new_color)
                existing_original_colors.add(new_color["original"])

        all_colors = existing_colors + colors_to_add
        colors_json = json.dumps({"colors": all_colors})

        # 2. Update
        cur.execute("""
            UPDATE products
            SET products = %s
            WHERE website_url = %s
        """, (colors_json, "colors"))
        conn.commit()
        cur.close()
        conn.close()

        print(f"✅ Colors row updated")
        print(f"📊 Total colors: {len(all_colors)} (Added {len(colors_to_add)} new colors)")

    except Exception as e:
        print(f"❌ Error updating colors row: {e}")

# -------------------------------------------------------
# TAGS
# -------------------------------------------------------
def upsert_tags_row(new_tags: list):
    try:
        conn, cur = get_connection()

        cur.execute("SELECT products FROM products WHERE website_url = %s", ("tags",))
        row = cur.fetchone()
        existing_tags = set()
        if row and row[0]:
            products_data = row[0]
            if isinstance(products_data, dict) and "tags" in products_data:
                existing_tags = set(products_data["tags"])

        combined_tags = sorted(existing_tags.union(set(new_tags)))
        tags_json = json.dumps({"tags": combined_tags})

        cur.execute("""
            INSERT INTO products (website_url, products)
            VALUES (%s, %s)
            ON CONFLICT (website_url)
            DO UPDATE SET products = EXCLUDED.products
        """, ("tags", tags_json))
        conn.commit()
        cur.close()
        conn.close()

        print(f"✅ Tags upserted ({len(combined_tags)} total tags).")

    except Exception as e:
        print(f"Error upserting tags: {e}")

# -------------------------------------------------------
# PRODUCTS
# -------------------------------------------------------
def upsert_product(product_json: dict, website_url: str, currency: str):
    try:
        conn, cur = get_connection()

        upsert_data = dict(product_json)
        upsert_data["website_url"] = website_url
        upsert_data["type"] = currency

        products_json = json.dumps(upsert_data.get("products"))
        total_products = upsert_data.get("total_products", "0")
        updated_at = upsert_data.get("updated_at", datetime.now().isoformat(sep=' ', timespec='seconds'))

        query = """
        INSERT INTO products (website_url, type, products, total_products, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (website_url)
        DO UPDATE SET
            type = EXCLUDED.type,
            products = EXCLUDED.products,
            total_products = EXCLUDED.total_products,
            updated_at = EXCLUDED.updated_at;
        """

        cur.execute(query, (website_url, currency, products_json, total_products, updated_at))
        conn.commit()
        cur.close()
        conn.close()

        print(f"Upsert successful for website_url '{website_url}'")

    except Exception as e:
        print(f"Error during upsert for website_url '{website_url}': {e}")

def get_product_by_website(website_url: str):
    try:
        conn, cur = get_connection()
        cur.execute("SELECT * FROM products WHERE website_url = %s", (website_url,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            return row
        else:
            print(f"No product found with website_url: {website_url}")
            return None

    except Exception as e:
        print(f"Error fetching product: {e}")
        return None

# -------------------------------------------------------
# ALL PRODUCT DATA
# -------------------------------------------------------
def upsert_all_product_data(cleaned_list, website_url, currency="USD"):
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

    # # Tags
    # all_tags = set()
    # for product in cleaned_list:
    #     tags_str = product.get("Tags", "")
    #     tags = [t.strip() for t in tags_str.split(",") if t.strip()]
    #     all_tags.update(tags)
    # upsert_tags_row(list(all_tags))

    # Colors
    extracted_colors = extract_colors_from_products({"products": cleaned_list})
    if extracted_colors:
        print("🔄 Updating colors in database...")
        update_colors_in_database(extracted_colors)
    else:
        print("⚠️ No colors found to update")

# -------------------------------------------------------
# Test
# -------------------------------------------------------
if __name__ == "__main__":
    print("Testing direct Postgres connection...")

    upsert_product(
        product_json={"products": {"products": []}, "total_products": "0"},
        website_url="talal",
        currency="pound"
    )

    print("\nFetching product by website_url 'talal':")
    product = get_product_by_website("talal")
    print(product)
