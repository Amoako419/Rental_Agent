import json
import re
import datetime
import boto3
import uuid # For generating unique IDs for listings if needed
import os
import time # For adding delays

# For loading .env file (pip install python-dotenv)
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# For S3 interaction (ensure you have 'boto3' installed)
# pip install boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from urllib.parse import urljoin # For making URLs absolute

# --- ADK Import ---
from google.adk.agents import Agent # Import the Agent class

# --- Load Environment Variables ---
load_dotenv()

# --- Configuration from Environment Variables ---
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN') # Optional
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1') # Default to 'us-east-1' if not set

S3_BUCKET_NAME_RAW = os.getenv('S3_BUCKET_NAME_RAW', 'ghana-apartments-raw-data-default')
S3_BUCKET_NAME_PROCESSED = os.getenv('S3_BUCKET_NAME_PROCESSED', 'ghana-apartments-processed-data-default')
# For currency conversion (example, ideally fetch dynamically or from config)
GHS_USD_EXCHANGE_RATE = os.getenv('GHS_USD_EXCHANGE_RATE', 14.5) 

# --- Boto3 Session ---
session_params = {
    'aws_access_key_id': AWS_ACCESS_KEY_ID,
    'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
    'region_name': AWS_REGION
}
if AWS_SESSION_TOKEN:
    session_params['aws_session_token'] = AWS_SESSION_TOKEN

boto_session = None # Initialize to None
try:
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY: 
        boto_session = boto3.Session(**session_params)
    else:
        print(f"[WARNING] {datetime.datetime.utcnow().isoformat()} - AWS credentials not fully configured in .env. S3 operations will be skipped.")
except Exception as e:
    print(f"[ERROR] {datetime.datetime.utcnow().isoformat()} - Failed to initialize Boto3 session: {e}. S3 operations will be skipped.")

# --- Logging Function (simple version) ---
def agent_log(level, message):
    """Simple logger for agent messages."""
    print(f"[{level.upper()}] {datetime.datetime.utcnow().isoformat()} - {message}")

# --- Helper Functions (Internal to the Agent's Tool) ---
def _understand_query(query_text):
    agent_log("info", f"Query Understanding - Processing query: '{query_text}'")
    entities = {
        "location": None, "bedrooms": None, "property_type": "apartment", "request_type": "rent_cost"
    }
    query_lower = query_text.lower()
    locations_ghana = [
        "east legon", "cantonments", "osu", "airport residential area", "airport hills",
        "labone", "roman ridge", "downtown accra", "spintex", "tema", "kumasi",
        "takoradi", "tesano", "dansoman", "adenta", "dome", "lapaz", "circle"
    ]
    for loc in locations_ghana:
        if loc in query_lower:
            entities["location"] = loc.title()
            break
    bedroom_match = re.search(r"(\d+)\s*(?:bed|bedroom|br)", query_lower)
    if bedroom_match: entities["bedrooms"] = int(bedroom_match.group(1))
    
    if "house" in query_lower or "bungalow" in query_lower or "villa" in query_lower:
        entities["property_type"] = "house"
    elif "townhouse" in query_lower: entities["property_type"] = "townhouse"
    elif "apartment" in query_lower or "flat" in query_lower: entities["property_type"] = "apartment"
    
    agent_log("info", f"Query Understanding - Extracted entities: {entities}")
    return entities

def _discover_sources(entities):
    agent_log("info", f"Source Discovery - Discovering sources for: {entities}")
    base_url_meqasa = "https://www.meqasa.com"
    target_urls = []
    search_parts_meqasa = []
    property_type_slug_meqasa = "properties"
    if entities.get("property_type") == "house": property_type_slug_meqasa = "houses"
    elif entities.get("property_type") == "apartment": property_type_slug_meqasa = "apartments"
    search_parts_meqasa.extend([property_type_slug_meqasa, "for-rent"])
    if entities.get("location"):
        search_parts_meqasa.append(f"in-{entities['location'].lower().replace(' ', '-')}")
    else:
        search_parts_meqasa.append("in-ghana")
    dynamic_url_meqasa = f"{base_url_meqasa}/{'-'.join(search_parts_meqasa)}"
    if entities.get("bedrooms"): dynamic_url_meqasa += f"?bed={entities['bedrooms']}"
    target_urls.append(dynamic_url_meqasa)
    if not target_urls: target_urls.append(f"{base_url_meqasa}/properties-for-rent-in-ghana")
    agent_log("info", f"Source Discovery - Identified potential URLs: {target_urls}")
    return target_urls

def _scrape_website_data(url):
    agent_log("info", f"Web Scraping - Scraping URL: {url}")
    headers = {
        'User-Agent': 'GhanaApartmentDataAgent/1.0 (+http://your-agent-contact-page.com; compatible; Googlebot-Image/1.0)',
        'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Connection': 'keep-alive'
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        agent_log("error", f"Web Scraping - Error fetching {url}: {e}")
        return []
    soup = BeautifulSoup(response.content, 'html.parser')
    listings_data = []
    site_name = "meqasa" if "meqasa.com" in url else "unknown"

    if site_name == "meqasa":
        property_cards = soup.find_all('article', class_=lambda x: x and 'mqs-prop-card' in x)
        if not property_cards:
             property_cards = soup.find_all('div', class_=lambda x: x and ('mqs-featured-prop-inner-wrap' in x or 'mqs-prop-card-premium' in x))
        agent_log("info", f"Web Scraping (Meqasa) - Found {len(property_cards)} potential listing elements on {url}.")
        for card_index, card in enumerate(property_cards):
            listing = {"id": str(uuid.uuid4()), "source_url": url, "scraped_date_utc": datetime.datetime.utcnow().isoformat()}
            try:
                price_el = card.find('span', class_='h3')
                if price_el: listing["price_raw"] = price_el.get_text(strip=True)
                loc_el = card.find('address')
                if loc_el: listing["location_raw"] = loc_el.get_text(strip=True)
                fur_are = card.find('div', class_='fur-are')
                if fur_are:
                    bed_el = fur_are.find('span', title=lambda t: t and 'bedroom' in t.lower())
                    if bed_el: listing["bedrooms_raw"] = bed_el.get_text(strip=True)
                    bath_el = fur_are.find('span', title=lambda t: t and 'bathroom' in t.lower())
                    if bath_el: listing["bathrooms_raw"] = bath_el.get_text(strip=True)
                title_link_el = card.find('a', class_=re.compile(r'mqs-prop-dt-wrapper|prop-title-link'))
                prop_type_el = card.find('div', class_='prop-type-card')
                if prop_type_el: listing["property_type_raw"] = prop_type_el.get_text(strip=True)
                if title_link_el:
                    listing["description_raw"] = title_link_el.get('title') or title_link_el.get_text(strip=True)
                    listing["listing_url"] = title_link_el.get('href')
                else:
                    header_el = card.find(['h2','h3','h4'], class_=re.compile(r'prop-title|card-title'))
                    if header_el:
                        listing["description_raw"] = header_el.get_text(strip=True)
                        parent_link = header_el.find_parent('a')
                        if parent_link and parent_link.has_attr('href'): listing["listing_url"] = parent_link['href']
                        elif header_el.find('a') and header_el.find('a').has_attr('href'): listing["listing_url"] = header_el.find('a')['href']
                if listing.get("listing_url") and not listing["listing_url"].startswith('http'):
                    listing["listing_url"] = urljoin(url, listing["listing_url"])
                if listing.get("price_raw") or listing.get("description_raw"): listings_data.append(listing)
            except Exception as e: agent_log("error", f"Web Scraping (Meqasa) - Error parsing card {card_index} on {url}: {e}")
    return listings_data

def _store_data_s3(data, bucket_name, object_name_prefix, session):
    if not data: return None
    if not session:
        agent_log("error", "S3 Storage - Boto3 session not initialized. Skipping S3 upload.")
        return None
    s3_client = session.client('s3')
    object_name = f"{object_name_prefix}/{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_{str(uuid.uuid4())}.json"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=json.dumps(data, indent=2), ContentType='application/json')
        s3_path = f"s3://{bucket_name}/{object_name}"
        agent_log("info", f"S3 Storage - Successfully stored data in S3: {s3_path}")
        return s3_path
    except Exception as e:
        agent_log("error", f"S3 Storage - Error storing data: {e}")
    return None

def _clean_and_transform_data(raw_listings_data):
    agent_log("info", f"Data Cleaning - Processing {len(raw_listings_data)} raw listings.")
    processed_listings = []
    for raw_listing in raw_listings_data:
        processed = raw_listing.copy()
        if raw_listing.get("price_raw"):
            price_text = raw_listing["price_raw"]
            currency, frequency = "GHS", "monthly"
            if "usd" in price_text.lower() or "$" in price_text: currency = "USD"
            if any(t in price_text.lower() for t in ["year", "/yr", "p.a."]): frequency = "yearly"
            elif any(t in price_text.lower() for t in ["week", "/wk"]): frequency = "weekly"
            price_numbers = re.findall(r"[\d\.,]+", price_text)
            if price_numbers:
                try: processed["price_numeric"] = float(price_numbers[0].replace(',', ''))
                except ValueError: processed["price_numeric"] = None
            processed.update({"price_currency": currency, "price_frequency": frequency})
        if raw_listing.get("location_raw"): processed["location_cleaned"] = raw_listing["location_raw"].split(',')[0].strip()
        for key_raw, key_numeric in [("bedrooms_raw", "bedrooms_numeric"), ("bathrooms_raw", "bathrooms_numeric")]:
            if raw_listing.get(key_raw):
                match = re.search(r"(\d+)", str(raw_listing[key_raw]))
                if match:
                    try: processed[key_numeric] = int(match.group(1))
                    except ValueError: processed[key_numeric] = None
        if raw_listing.get("property_type_raw"):
            pt_raw = raw_listing["property_type_raw"].lower()
            if any(t in pt_raw for t in ["apartment", "flat"]): processed["property_type_cleaned"] = "apartment"
            elif any(t in pt_raw for t in ["house", "bungalow", "villa", "detached"]): processed["property_type_cleaned"] = "house"
            elif "townhouse" in pt_raw: processed["property_type_cleaned"] = "townhouse"
            else: processed["property_type_cleaned"] = pt_raw
        processed.update({"_is_processed": True, "_processed_date_utc": datetime.datetime.utcnow().isoformat()})
        processed_listings.append(processed)
    agent_log("info", f"Data Cleaning - Finished. {len(processed_listings)} listings cleaned.")
    return processed_listings

def _analyze_and_retrieve_data(data_source, query_entities, session):
    agent_log("info", f"Data Analysis - Analyzing for: {query_entities}")
    all_processed_listings = []
    if isinstance(data_source, list): all_processed_listings = data_source
    elif isinstance(data_source, str) and data_source.startswith('s3://'):
        if not session: return "Error: AWS config missing for S3 load."
        try:
            s3_client, bucket_name, object_key = session.client('s3'), data_source.split('/')[2], '/'.join(data_source.split('/')[3:])
            all_processed_listings = json.loads(s3_client.get_object(Bucket=bucket_name, Key=object_key)['Body'].read().decode('utf-8'))
        except Exception as e: return f"Error: Could not load from S3 {data_source}: {e}"
    if not all_processed_listings: return "No data found to analyze."
    
    filtered_listings = []
    for listing in all_processed_listings:
        match = True
        if query_entities.get("location") and query_entities["location"].lower() not in listing.get("location_cleaned", "").lower(): match = False
        if query_entities.get("bedrooms") is not None and listing.get("bedrooms_numeric") != query_entities["bedrooms"]: match = False
        if query_entities.get("property_type") and query_entities["property_type"].lower() != listing.get("property_type_cleaned", "").lower(): match = False
        if match: filtered_listings.append(listing)
    
    if not filtered_listings: return f"No listings found for: Loc='{query_entities.get('location', 'any')}', Beds='{query_entities.get('bedrooms', 'any')}', Type='{query_entities.get('property_type', 'any')}'."

    monthly_ghs_prices = []
    try: ghs_usd = float(GHS_USD_EXCHANGE_RATE)
    except ValueError: ghs_usd = 14.5 # Fallback
    
    for listing in filtered_listings:
        price, currency, freq = listing.get("price_numeric"), listing.get("price_currency"), listing.get("price_frequency")
        if price is None: continue
        
        # Convert to monthly GHS
        monthly_price_ghs = None
        if currency == "GHS":
            if freq == "monthly": monthly_price_ghs = price
            elif freq == "yearly": monthly_price_ghs = price / 12
            elif freq == "weekly": monthly_price_ghs = price * 4.33 # Approx weeks in month
            elif freq == "daily": monthly_price_ghs = price * 30 # Approx days in month
        elif currency == "USD":
            price_ghs_equivalent = price * ghs_usd
            if freq == "monthly": monthly_price_ghs = price_ghs_equivalent
            elif freq == "yearly": monthly_price_ghs = price_ghs_equivalent / 12
            # Add weekly/daily for USD if needed
        
        if monthly_price_ghs is not None:
            monthly_ghs_prices.append(monthly_price_ghs)

    if not monthly_ghs_prices: return f"Found {len(filtered_listings)} listings, but none with parseable GHS-equivalent monthly pricing."
    
    avg_price, min_price, max_price = sum(monthly_ghs_prices) / len(monthly_ghs_prices), min(monthly_ghs_prices), max(monthly_ghs_prices)
    return (f"Found {len(filtered_listings)} listings ({len(monthly_ghs_prices)} with GHS monthly prices). "
            f"For {query_entities.get('bedrooms','any')}-BR {query_entities.get('property_type','properties')} "
            f"in {query_entities.get('location','Ghana')}: "
            f"Avg Rent: GHS {avg_price:,.2f}/month. Range: GHS {min_price:,.2f} - GHS {max_price:,.2f}/month.")


# --- ADK Tool Definition ---
def get_ghana_apartment_data(user_query: str) -> dict:
    """
    Retrieves and analyzes apartment rental data from Ghana based on a user query.
    It scrapes data from online sources, cleans it, stores it in S3 (if configured),
    and provides an analysis of rental prices and availability.
    Args:
        user_query (str): The user's question about Ghana apartment rentals.
    Returns:
        dict: A dictionary containing the status ('success' or 'error') and
              either a 'report' with the findings or an 'error_message'.
    """
    agent_log("info", f"--- Ghana Apartment Data Tool started for query: '{user_query}' ---")
    # Use the global boto_session
    session = boto_session 
    tool_result = {"status": "processing", "report": "", "error_message": ""}

    entities = _understand_query(user_query)
    if not entities.get("location") and entities.get("bedrooms") is None and not entities.get("property_type"):
        tool_result = {"status": "error", "error_message": "Could not understand key details. Please specify location, bedrooms, or property type."}
        agent_log("warning", tool_result["error_message"])
        return tool_result

    source_urls = _discover_sources(entities)
    if not source_urls:
        tool_result = {"status": "error", "error_message": "Could not identify online sources for your query."}
        agent_log("warning", tool_result["error_message"])
        return tool_result

    all_raw_data = []
    for url in source_urls:
        agent_log("info", f"Tool - Will attempt to scrape: {url}")
        time.sleep(2) 
        raw_data_from_url = _scrape_website_data(url)
        if raw_data_from_url: all_raw_data.extend(raw_data_from_url)
    
    s3_data_paths = {}
    if all_raw_data:
        agent_log("info", f"Tool - Scraped {len(all_raw_data)} raw listings.")
        if session:
            raw_s3_path = _store_data_s3(all_raw_data, S3_BUCKET_NAME_RAW, "raw_listings", session)
            if raw_s3_path: s3_data_paths["raw_s3_path"] = raw_s3_path
    else:
        agent_log("warning", "Tool - No data was scraped from identified sources.")

    processed_listings = _clean_and_transform_data(all_raw_data)
    if processed_listings:
        if session:
            processed_s3_path = _store_data_s3(processed_listings, S3_BUCKET_NAME_PROCESSED, "processed_listings", session)
            if processed_s3_path: s3_data_paths["processed_s3_path"] = processed_s3_path
    else:
        agent_log("info", "Tool - No new data was processed.")

    analysis_report = ""
    if processed_listings:
        analysis_report = _analyze_and_retrieve_data(processed_listings, entities, session)
    else:
        agent_log("info", "Tool - No fresh data. Attempting to load latest historical from S3.")
        if not session:
            analysis_report = "No fresh data; AWS config missing for historical data access."
        else:
            try:
                s3_res, bucket = session.resource('s3'), session.resource('s3').Bucket(S3_BUCKET_NAME_PROCESSED)
                latest_file_key, latest_mod_time = None, None
                prefix_to_check = "processed_listings/"
                if not S3_BUCKET_NAME_PROCESSED: raise ValueError("S3_BUCKET_NAME_PROCESSED not configured.")
                for obj_summary in bucket.objects.filter(Prefix=prefix_to_check):
                    if obj_summary.key.endswith('.json'):
                        if latest_mod_time is None or obj_summary.last_modified > latest_mod_time:
                            latest_mod_time, latest_file_key = obj_summary.last_modified, obj_summary.key
                if latest_file_key:
                    s3_path_to_load = f"s3://{S3_BUCKET_NAME_PROCESSED}/{latest_file_key}"
                    analysis_report = _analyze_and_retrieve_data(s3_path_to_load, entities, session)
                else:
                    analysis_report = "No fresh data available, and no historical data found in S3."
            except Exception as e:
                analysis_report = f"Error accessing historical S3 data: {e}"

    if "Error:" in analysis_report or "No data" in analysis_report or "No listings" in analysis_report:
        tool_result = {"status": "error", "error_message": analysis_report, "s3_paths": s3_data_paths}
    else:
        tool_result = {"status": "success", "report": analysis_report, "s3_paths": s3_data_paths}
    
    agent_log("info", f"--- Ghana Apartment Data Tool finished. Status: {tool_result['status']} ---")
    return tool_result

# --- ADK Agent Definition ---
root_agent = Agent(
    name="ghana_rental_analyzer",
    model="gemini-2.0-flash-lite", # Or a more capable model like "gemini-pro"
    description=(
        "Provides information about apartment and house rentals in Ghana, "
        "including average prices, price ranges, and availability based on location and number of bedrooms. "
        "It scrapes data from Ghanaian real estate websites."
    ),
    instruction=(
        "You are an expert real estate agent for Ghana. "
        "Use the 'get_ghana_apartment_data' tool to answer user questions about rental properties. "
        "Provide a concise summary of the findings from the tool's report. "
        "If the tool returns an error or no data, inform the user clearly."
        "Always state prices in GHS (Ghanaian Cedis)."
    ),
    tools=[get_ghana_apartment_data],
    # enable_interpreter_tool=False, # Explicitly disable if not needed
    # enable_search_tool=False,     # Explicitly disable if not needed
)

if __name__ == '__main__':
    agent_log("info", "--- Agent Script Started (Direct Execution of Tool) ---")
    if boto_session is None:
        agent_log("critical", "Boto3 session not initialized. S3 ops will be skipped.")
    else:
        agent_log("info", "Boto3 session initialized.")

    # Example User Queries for direct tool testing
    query1 = "How much does rent cost in Osu for a 2-bedroom apartment?"
    # query1 = "show me rent for 2 bedroom apartment in Osu"
    # query2 = "4 bed house in Cantonments price"
    # query3 = "what is the average rent for a 1 bedroom in Airport Residential in USD" # Tool now converts to GHS
    # query4 = "any houses for rent in West Legon"

    # Test the tool function directly
    result = get_ghana_apartment_data(query1) 

    print("\n--- Tool Direct Execution Output ---")
    print(json.dumps(result, indent=2))

    agent_log("info", "--- Agent Script Ended (Direct Execution of Tool) ---")
