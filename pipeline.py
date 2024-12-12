import requests
import json
import os
import pandas as pd

# Initialize final dictionaries
final_category_json = {"category_results": []}
final_search_json = {"search_results": []}
final_products_json = {"product": []}
final_offers_json = {"product": [], "offers": []}

# API Configuration
base_url = "https://api.asindataapi.com/request"
API_KEY = "0F1F6FDD22E9480A81FE60385C18C2BF"
OUTPUT_DIR = "output/staging/raw"
TRANSFORMED_OUTPUT_DIR = "output/staging/transformed"

# API Call
def api_call(params, endpoint):
    """Make an API call and return the JSON response."""
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling {endpoint}: {e}")
        return None

# Save JSON
def save_json(data, file_path):
    """Save data to a JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

# Extract ASIN from category or search
def extract_asins(folder_path):
    """Extract ASINs from multiple pages of search JSON files."""
    asins = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, "r") as file:
                data = json.load(file)
                if "category_results" in data:
                    asins.extend(item.get("asin") for item in data["category_results"])
                if "search_results" in data:
                    asins.extend(item.get("asin") for item in data["search_results"])
    return asins

######################
# 1. Extract Stage
######################
def extract_stage(pages_to_extract=4):
    for page in range(1, pages_to_extract + 1):
        # Category API Call
        category_params = {
            'api_key': API_KEY,
            'type': 'category',
            'amazon_domain': 'amazon.com',
            'category_id': '565108',  # Laptop Category
            'page': page,
        }
        print(f"Extracting Category Data for Page {page}...")
        category_data = api_call(category_params, "category")
        if category_data and "category_results" in category_data:
            final_category_json["category_results"].extend(category_data["category_results"])
            save_json(category_data, f"{OUTPUT_DIR}/category/page_{page}.json")

        # Search API Call
        search_params = {
            'api_key': API_KEY,
            'type': 'search',
            'url': 'https://www.amazon.com/s?i=computers&rh=n%3A565108%2Cp_123%3A110955%7C219979%7C220854&dc',
            'page': page,
        }
        print(f"Extracting Search Data for Page {page}...")
        search_data = api_call(search_params, "search")
        if search_data and "search_results" in search_data:
            final_search_json["search_results"].extend(search_data["search_results"])
            save_json(search_data, f"{OUTPUT_DIR}/search/page_{page}.json")

    save_json(final_category_json, f"{OUTPUT_DIR}/category/final_category.json")
    save_json(final_search_json, f"{OUTPUT_DIR}/search/final_search.json")

    asins = extract_asins(f"{OUTPUT_DIR}/search/")
    print(f"Extracted {len(asins)} ASINs: {asins}")

    for asin in asins:
        product_params = {
            'api_key': API_KEY,
            'type': 'product',
            'amazon_domain': 'amazon.com',
            'asin': asin
        }
        print(f"Extracting Product Data for ASIN {asin}...")
        product_response = api_call(product_params, "product")
        if product_response:
            save_json(product_response, f"{OUTPUT_DIR}/products/product_{asin}.json")
            final_products_json["product"].append(product_response["product"])

        offers_params = {
            'api_key': API_KEY,
            'type': 'offers',
            'amazon_domain': 'amazon.com',
            'asin': asin
        }
        print(f"Extracting Offers Data for ASIN {asin}...")
        offers_response = api_call(offers_params, "offers")
        if offers_response:
            save_json(offers_response, f"{OUTPUT_DIR}/offers/offers_{asin}.json")
            final_offers_json["product"].append(offers_response["product"])
            final_offers_json["offers"].extend(offers_response["offers"])

    save_json(final_products_json, f"{OUTPUT_DIR}/products/final_products.json")
    save_json(final_offers_json, f"{OUTPUT_DIR}/offers/final_offers.json")

######################
# 2. Transform Stage
######################
def transform_stage():
    """Transform raw JSON files into structured CSV files."""
    product_data = []
    for file_name in os.listdir(f"{OUTPUT_DIR}/products"):
        if file_name.endswith(".json"):
            with open(f"{OUTPUT_DIR}/products/{file_name}", "r") as file:
                data = json.load(file)
                product = data.get("product")
                if product:
                    product_data.append({
                        "asin": product.get("asin"),
                        "brand": product.get("brand"),
                        "title": product.get("title"),
                        "is_prime": product.get("buybox_winner", {}).get("is_prime"),
                        "is_sold_by_amazon": product.get("buybox_winner", {}).get("fulfillment", {}).get("is_sold_by_amazon"),
                        "is_new": product.get("buybox_winner", {}).get("condition", {}).get("is_new"),
                        "rating": product.get("rating"),
                        "ratings_total": product.get("ratings_total"),
                        "price": product.get("buybox_winner", {}).get("price", {}).get("value"),
                        "delivery_fee": product.get("buybox_winner", {}).get("shipping", {}).get("raw"),
                        "delivery_date": product.get("buybox_winner", {}).get("fulfillment", {}).get("standard_delivery", {}).get("date"),
                        "five_star_percentage": product.get("rating_breakdown", {}).get("five_star", {}).get("percentage"),
                        "five_star_count": product.get("rating_breakdown", {}).get("five_star", {}).get("count"),
                        "four_star_percentage": product.get("rating_breakdown", {}).get("four_star", {}).get("percentage"),
                        "four_star_count": product.get("rating_breakdown", {}).get("four_star", {}).get("count"),
                        "three_star_percentage": product.get("rating_breakdown", {}).get("three_star", {}).get("percentage"),
                        "three_star_count": product.get("rating_breakdown", {}).get("three_star", {}).get("count"),
                        "two_star_percentage": product.get("rating_breakdown", {}).get("two_star", {}).get("percentage"),
                        "two_star_count": product.get("rating_breakdown", {}).get("two_star", {}).get("count"),
                        "one_star_percentage": product.get("rating_breakdown", {}).get("one_star", {}).get("percentage"),
                        "one_star_count": product.get("rating_breakdown", {}).get("one_star", {}).get("count"),
                        "reviews": [
                            {
                                "review_id": review.get("id"),
                                "review_text": review.get("text"),
                                "review_rating": review.get("rating"),
                                "reviewer_name": review.get("profile", {}).get("name"),
                                "review_country": review.get("review_country")
                            }
                            for review in product.get("top_reviews", [])
                        ],
                        "variants": product.get("variants"),
                        "protection_plans": product.get("protection_plans")
                    })

    pd.DataFrame(product_data).to_csv(f"{TRANSFORMED_OUTPUT_DIR}/products.csv", index=False)

    offer_data = []
    for file_name in os.listdir(f"{OUTPUT_DIR}/offers"):
        if file_name.endswith(".json"):
            with open(f"{OUTPUT_DIR}/offers/{file_name}", "r") as file:
                data = json.load(file)
                offers = data.get("offers")
                if offers:
                    for offer in offers:
                        offer_data.append({
                            "asin": data.get("product", {}).get("asin"),
                            "price": offer.get("price", {}).get("value"),
                            "condition": offer.get("condition", {}).get("title"),
                            "is_prime": offer.get("is_prime"),
                            "fulfilled_by_amazon": offer.get("delivery", {}).get("fulfilled_by_amazon"),
                            "expected_delivery": offer.get("delivery", {}).get("comments"),
                            "seller_name": offer.get("seller", {}).get("name")
                        })

    pd.DataFrame(offer_data).to_csv(f"{TRANSFORMED_OUTPUT_DIR}/offers.csv", index=False)

# Run the pipeline
if __name__ == "__main__":
    extract_stage()
    transform_stage()
