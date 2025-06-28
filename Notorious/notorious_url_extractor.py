import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin, urlparse
import os

class NotoriousPlugScraper:
    def __init__(self, base_url="https://notorious-plug.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def get_page_content(self, url):
        """Fetch page content with error handling and retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def get_total_pages(self, collection_url):
        """Extract total number of pages from pagination - FIXED VERSION"""
        print(f"Determining total pages for: {collection_url}")
        
        content = self.get_page_content(collection_url)
        if not content:
            return 1
            
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for pagination elements
        pagination = soup.find('div', class_='Pagination')
        if not pagination:
            print("No pagination found, assuming single page")
            return 1
        
        # Find all pagination nav items
        nav_items = pagination.find_all(['a', 'span'], class_='Pagination__NavItem')
        
        # Look for the last page number before the "next" link
        last_page_number = 1
        
        for i, item in enumerate(nav_items):
            # Check if this is a "next" link
            if item.get('rel') == 'next' or (item.name == 'a' and 'next' in item.get('title', '').lower()):
                # The previous item should be the last page number
                if i > 0:
                    prev_item = nav_items[i - 1]
                    if prev_item.name == 'a':
                        href = prev_item.get('href', '')
                        # Extract page number from href
                        match = re.search(r'page=(\d+)', href)
                        if match:
                            last_page_number = int(match.group(1))
                            break
                    elif prev_item.name == 'span':
                        # If it's a span, get the text content
                        text = prev_item.get_text(strip=True)
                        if text.isdigit():
                            last_page_number = int(text)
                            break
        
        # If we didn't find it through the next link method, try alternative approach
        if last_page_number == 1:
            # Look for all page numbers and take the highest one
            page_numbers = []
            for item in nav_items:
                if item.name == 'a':
                    href = item.get('href', '')
                    if 'page=' in href:
                        match = re.search(r'page=(\d+)', href)
                        if match:
                            page_num = int(match.group(1))
                            if page_num > 0 and page_num <= 1000:  # Sanity check
                                page_numbers.append(page_num)
                elif item.name == 'span':
                    text = item.get_text(strip=True)
                    if text.isdigit() and text != '…':
                        page_num = int(text)
                        if page_num > 0 and page_num <= 1000:  # Sanity check
                            page_numbers.append(page_num)
            
            if page_numbers:
                last_page_number = max(page_numbers)
        
        print(f"Found {last_page_number} total pages")
        return last_page_number
    
    def get_collection_tags(self, collection_name):
        """Get tags for a specific collection"""
        tag_mapping = {
            'womens': ['Women', 'All Women'],
            'clothing': ['Men', 'All Men'],
            'shoes': ['Men', 'All Men', 'Mens Shoes']
        }
        return tag_mapping.get(collection_name, [])
    
    def extract_product_links(self, page_content, base_url, collection_tags=None):
        """Extract product links and information from a page"""
        soup = BeautifulSoup(page_content, 'html.parser')
        products = []
        
        # Find all product items
        product_items = soup.find_all('div', class_='ProductItem__Info')
        
        for item in product_items:
            try:
                # Extract product title and link
                title_element = item.find('h2', class_='ProductItem__Title')
                if title_element:
                    link_element = title_element.find('a')
                    if link_element:
                        product_url = link_element.get('href', '')
                        product_title = link_element.get_text(strip=True)
                        
                        # Make URL absolute
                        if product_url.startswith('/'):
                            product_url = urljoin(base_url, product_url)
                        
                        # Extract pricing information
                        price_list = item.find('div', class_='ProductItem__PriceList')
                        current_price = ""
                        compare_price = ""
                        
                        if price_list:
                            # Current price (highlighted)
                            current_price_elem = price_list.find('span', class_='ProductItem__Price Price--highlight')
                            if current_price_elem:
                                current_price = current_price_elem.get_text(strip=True)
                            
                            # Compare price (original price)
                            compare_price_elem = price_list.find('span', class_='ProductItem__Price Price--compareAt')
                            if compare_price_elem:
                                compare_price = compare_price_elem.get_text(strip=True)
                        
                        product_data = {
                            'title': product_title,
                            'url': product_url,
                            'current_price': current_price,
                            'compare_price': compare_price,
                            'tags': collection_tags or [],  # Add tags to product data
                            'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        products.append(product_data)
                        print(f"  ✓ Extracted: {product_title}")
                        
            except Exception as e:
                print(f"Error extracting product: {e}")
                continue
        
        return products
    
    def scrape_collection(self, collection_path):
        """Scrape all pages of a collection"""
        collection_url = urljoin(self.base_url, collection_path)
        print(f"\n{'='*60}")
        print(f"Starting scrape of collection: {collection_url}")
        print(f"{'='*60}")
        
        # Get total number of pages
        total_pages = self.get_total_pages(collection_url)
        
        all_products = []
        seen_urls = set()  # Track seen URLs to avoid duplicates
        seen_titles = set()  # Track seen titles to avoid duplicates
        
        # Scrape each page
        for page_num in range(1, total_pages + 1):
            print(f"\nScraping page {page_num}/{total_pages}")
            
            # Construct page URL
            if page_num == 1:
                page_url = collection_url
            else:
                page_url = f"{collection_url}?page={page_num}"
            
            # Get page content
            content = self.get_page_content(page_url)
            if not content:
                print(f"Failed to get content for page {page_num}")
                continue
            
            # Extract products from this page
            products = self.extract_product_links(content, self.base_url, self.get_collection_tags(collection_path.split('/')[-1]))
            
            # If no products found and this is page 2 or later, we might have reached the end
            if not products and page_num > 1:
                print(f"No products found on page {page_num}, stopping pagination")
                break
            
            # Filter out duplicates based on URL and title
            new_products = []
            duplicates_found = 0
            
            for product in products:
                product_url = product['url']
                product_title = product['title'].strip().lower()  # Normalize title for comparison
                
                if product_url in seen_urls:
                    print(f"  ⚠ Skipped duplicate URL: {product['title']}")
                    duplicates_found += 1
                elif product_title in seen_titles:
                    print(f"  ⚠ Skipped duplicate title: {product['title']}")
                    duplicates_found += 1
                else:
                    seen_urls.add(product_url)
                    seen_titles.add(product_title)
                    new_products.append(product)
            
            all_products.extend(new_products)
            
            print(f"Found {len(products)} products on page {page_num} ({len(new_products)} new, {duplicates_found} duplicates)")
            
            # Add delay between requests to be respectful
            if page_num < total_pages:
                time.sleep(1)
        
        print(f"\nTotal products extracted from {collection_path}: {len(all_products)} (unique URLs and titles)")
        return all_products
    
    def save_to_json(self, all_collections_data, filename="all_url.json"):
        """Save extracted products from all collections to JSON file"""
        data = {
            'base_url': self.base_url,
            'total_collections': len(all_collections_data),
            'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'collections': all_collections_data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\nData saved to {filename}")
        return filename

def main():
    """Main function to run the scraper for multiple collections"""
    scraper = NotoriousPlugScraper()
    
    # Define the collections to scrape
    collections = [
        "/collections/womens",
        "/collections/clothing", 
        "/collections/shoes"
    ]
    
    all_collections_data = {}
    total_products = 0
    global_seen_urls = set()  # Track URLs across all collections
    global_seen_titles = set()  # Track titles across all collections
    
    # Scrape each collection
    for collection_path in collections:
        collection_name = collection_path.split('/')[-1]
        print(f"\n{'='*60}")
        print(f"SCRAPING COLLECTION: {collection_name.upper()}")
        print(f"{'='*60}")
        
        products = scraper.scrape_collection(collection_path)
        
        # Filter out products that already exist in other collections (by URL or title)
        unique_products = []
        for product in products:
            product_url = product['url']
            product_title = product['title'].strip().lower()  # Normalize title for comparison
            
            # Check if product already exists by URL or title
            if product_url in global_seen_urls:
                print(f"  ⚠ Global duplicate skipped (URL): {product['title']} (already in another collection)")
                continue
            elif product_title in global_seen_titles:
                print(f"  ⚠ Global duplicate skipped (Title): {product['title']} (already in another collection)")
                continue
            else:
                # Add to tracking sets and keep the product
                global_seen_urls.add(product_url)
                global_seen_titles.add(product_title)
                unique_products.append(product)
        
        all_collections_data[collection_name] = {
            'collection_url': urljoin(scraper.base_url, collection_path),
            'total_products': len(unique_products),
            'products': unique_products
        }
        
        total_products += len(unique_products)
        
        print(f"Completed {collection_name}: {len(unique_products)} unique products")
    
    # Save all data to JSON file
    scraper.save_to_json(all_collections_data, "all_url.json")
    
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"Total collections scraped: {len(collections)}")
    print(f"Total unique products extracted: {total_products}")
    print(f"Total unique URLs across all collections: {len(global_seen_urls)}")
    print(f"Total unique titles across all collections: {len(global_seen_titles)}")
    
    # Print summary for each collection
    for collection_name, data in all_collections_data.items():
        print(f"{collection_name}: {data['total_products']} unique products")

if __name__ == "__main__":
    main()
