# run.py
"""
Main script to run any scraper or conversion utility.
Comment or uncomment the lines below to run the desired workflow.
"""

# --- Cruise Fashion Scraper ---
# from scrapers.cruise_fashion.cruise_fashion import complete_workflow
# complete_workflow()

# --- Shopify CSV Conversion from DB ---
# from json_to_shopify_csv import convert_website_to_shopify_csv
# convert_website_to_shopify_csv("https://www.cruisefashion.com")

# --- Add more scrapers/converters below as needed --- 

# from json_to_shopify_csv import convert_website_to_shopify_csv
# convert_website_to_shopify_csv("https://notorious-plug.com")

# from scrapers.Notorious.notorious import complete_workflow
# complete_workflow()



from scrapers.youngla.youngla import complete_workflow
collections = [
    {"url": "https://www.youngla.com/collections/tanks", "gender": "men", "product_type": "tanks"},
    {"url": "https://www.youngla.com/collections/t-shirts", "gender": "men", "product_type": "shirts"},
    {"url": "https://www.youngla.com/collections/long-sleeves-for-him", "gender": "men", "product_type": "long sleeves"},
    {"url": "https://www.youngla.com/collections/shorts", "gender": "men", "product_type": "shorts"},
    # {"url": "https://www.youngla.com/collections/jeans", "gender": "men", "product_type": "pants"},
    # {"url": "https://www.youngla.com/collections/outerwear", "gender": "men", "product_type": "outerwear"},
    # {"url": "https://www.youngla.com/collections/joggers", "gender": "men", "product_type": "joggers"},
    # {"url": "https://www.youngla.com/collections/hats", "gender": "men", "product_type": "hats"},
    # {"url": "https://www.youngla.com/collections/lifting-gear", "gender": "men", "product_type": "accessories"},
]

#     collections += [
#     {"url": "https://www.youngla.com/collections/bras", "gender": "women", "product_type": "bras"},
#     {"url": "https://www.youngla.com/collections/shirts", "gender": "women", "product_type": "tops"},
#     {"url": "https://www.youngla.com/collections/bodysuits", "gender": "women", "product_type": "bodysuits"},
#     {"url": "https://www.youngla.com/collections/shorts-1", "gender": "women", "product_type": "shorts"},
#     {"url": "https://www.youngla.com/collections/joggers-1", "gender": "women", "product_type": "leggings"},
#     {"url": "https://www.youngla.com/collections/joggers-pants-for-her", "gender": "women", "product_type": "joggers"},
#     {"url": "https://www.youngla.com/collections/outwear", "gender": "women", "product_type": "outerwear"},
#     {"url": "https://www.youngla.com/collections/tanks-1", "gender": "women", "product_type": "matching sets"},
#     {"url": "https://www.youngla.com/collections/accessories-for-her", "gender": "women", "product_type": "accessories"},
# ]


complete_workflow(collections)


