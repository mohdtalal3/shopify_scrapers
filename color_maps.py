from google import genai
from supabase import create_client, Client
import json
import re
import os
from dotenv import load_dotenv
load_dotenv()
import time


url = os.getenv("SUPABASE_URL")
service_key = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, service_key)
client=genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Define the prompt template
PROMPT_TEMPLATE = """
You are a color‑mapping assistant.

**Task:**  
Given a user‑supplied list of colors (names or descriptive phrases, any format), you will:

1. Parse each color input.
2. Find the closest matching HTML/CSS named color.
3. Output a JSON object containing:

- "original": the user's exact input string  
- "mapped": the HTML named color  

Return a JSON array under the key `"colors"`.

**Rules:**  
– Always output valid JSON.  
– Match the color meaningfully (e.g. "shiny rose gold" → "RosyBrown" or closest).  
– No extra commentary—just the JSON.

**Example Input:**  
["shiny rose gold", "light purple brown mirror", "shiny gold ochre", "shiny light gold dark grey"]

**Expected Output Format:**
```json
{
  "colors": [
    {
      "original": "shiny rose gold",
      "mapped": "RosyBrown"
    },
    {
      "original": "light purple brown mirror",
      "mapped": "RosyBrown"
    }
  ]
}
```

Now, do the same for the following:
{colors}
"""

# --- Fetch unmapped colors from DB ---
def get_unmapped_colors_from_db() -> list[str]:
    """
    Fetches the list of original color names from the database where mapped == "".
    Returns a list of color strings needing mapping.
    """
    try:
        response = supabase.table("products").select("products").eq("website_url", "colors").execute()
        if not response.data or not response.data[0].get("products"):
            return []
        products_data = response.data[0]["products"]
        if not isinstance(products_data, dict) or "colors" not in products_data:
            return []
        colors = products_data["colors"]
        # Only return original where mapped is empty
        unmapped = [c["original"] for c in colors if isinstance(c, dict) and c.get("mapped", "") == ""]
        return unmapped
    except Exception as e:
        print(f"Error fetching unmapped colors: {e}")
        return []

# --- Update mapped colors in DB ---
def update_mapped_colors_in_db(mapped_colors: list[dict]):
    """
    Updates the mapped values for the given original colors in the database.
    Only updates the colors in the batch, leaves others untouched.
    """
    try:
        # Fetch the current colors row
        response = supabase.table("products").select("products").eq("website_url", "colors").execute()
        if not response.data or not response.data[0].get("products"):
            print("No colors row found in DB.")
            return
        products_data = response.data[0]["products"]
        if not isinstance(products_data, dict) or "colors" not in products_data:
            print("Malformed colors row in DB.")
            return
        colors = products_data["colors"]

        # Build a mapping from original to mapped for this batch
        batch_map = {c["original"]: c["mapped"] for c in mapped_colors if c.get("mapped")}

        # Update only the mapped values for originals in this batch
        for color in colors:
            if color.get("original") in batch_map:
                color["mapped"] = batch_map[color["original"]]

        # Write back to DB
        updated_json = {"colors": colors}
        supabase.table("products").update({"products": updated_json}).eq("website_url", "colors").execute()
        print(f"✅ Updated {len(batch_map)} mapped colors in DB.")
    except Exception as e:
        print(f"Error updating mapped colors in DB: {e}")

# --- Helper to clean AI response ---
def extract_json_from_response(response: str) -> str:
    # Remove markdown code block markers and whitespace
    cleaned = re.sub(r"^```json|^```|```$", "", response.strip(), flags=re.MULTILINE).strip()
    return cleaned



def map_colors_to_html(colors: list[str]) -> str:
    response = client.models.generate_content(model="gemini-2.5-flash-lite-preview-06-17",contents=PROMPT_TEMPLATE.replace("{colors}", str(colors)))
    return response.text or ""


def run_color_mapping():
    unmapped_colors = get_unmapped_colors_from_db()
    print("Unmapped colors needing mapping:", len(unmapped_colors))

    batch_size = 100
    for i in range(0, len(unmapped_colors), batch_size):
        batch = unmapped_colors[i:i+batch_size]
        print(f"\nMapping batch {i//batch_size + 1} ({len(batch)} colors)...")
        result = map_colors_to_html(batch)
        #print(result)
        # Parse the Gemini response and update DB
        try:
            cleaned_result = extract_json_from_response(result)
            mapped = json.loads(cleaned_result)
            if "colors" in mapped:
                update_mapped_colors_in_db(mapped["colors"])
        except Exception as e:
            print(f"Error parsing or updating batch: {e}")
        time.sleep(5)

# Example usage
if __name__ == "__main__":
    run_color_mapping()