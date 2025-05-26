# Main Python file for the Ghana Apartment Data Agent (Conceptual)
# This script outlines the core components and their interactions.
# It uses common Python libraries to illustrate the functionality.

import json
import re
import datetime
import uuid # For generating unique IDs for listings if needed
# For web scraping (ensure you have 'requests' and 'beautifulsoup4' installed)
# pip install requests beautifulsoup4
import requests
from bs4 import BeautifulSoup

# For S3 interaction (ensure you have 'boto3' installed)
# pip install boto3
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# --- Configuration ---
# (In a real application, these would come from a config file or environment variables)
S3_BUCKET_NAME_RAW = 'ghana-apartments-raw-data' # Replace with your actual S3 bucket for raw data
S3_BUCKET_NAME_PROCESSED = 'ghana-apartments-processed-data' # Replace with your S3 bucket for processed data
AWS_REGION = 'your-aws-region' # e.g., 'us-east-1'. Boto3 might pick this up from your AWS config.

# --- 1. Query Understanding Module (Simplified) ---
def understand_query(query_text):
    """
    Parses a natural language query to extract key entities.
    This is a very simplified version. Real-world NLP is more complex
    and might use libraries like spaCy, NLTK, or LLMs.
    """
    print(f"\n[Query Understanding] Processing query: '{query_text}'")
    entities = {
        "location": None,
        "bedrooms": None,
        "property_type": "apartment", # Default, can be refined
        "request_type": "rent_cost" # Default, can be refined
    }

    # Simple keyword-based extraction
    query_lower = query_text.lower()

    # Location extraction (very basic, needs a proper gazetteer or NER for robustness)
    locations_ghana = ["east legon", "cantonments", "osu", "airport residential", "downtown accra", "tema", "kumasi"]
    for loc in locations_ghana:
        if loc in query_lower:
            entities["location"] = loc.title() # Capitalize for consistency
            break

    # Bedroom extraction
    bedroom_match = re.search(r"(\d+)\s*(?:bed|bedroom|br)", query_lower)
    if bedroom_match:
        entities["bedrooms"] = int(bedroom_match.group(1))

    # Property type (example)
    if "house" in query_lower:
        entities["property_type"] = "house"
    elif "townhouse" in query_lower:
        entities["property_type"] = "townhouse"

    print(f"[Query Understanding] Extracted entities: {entities}")
    return entities

# --- 2. Web Search & Source Discovery Tool (Conceptual - normally an external tool or API) ---
def discover_sources(entities):
    """
    Identifies potential online sources based on entities.
    In a real ADK, this might call a search API.
    Here, we'll simulate it with a predefined list for a known portal.
    """
    print(f"\n[Source Discovery] Discovering sources for: {entities}")
    # This is highly simplified. Real discovery would involve dynamic web searches.
    # Example: if entities['location'] is "East Legon", construct search URLs.
    # For now, let's assume we have a primary target site.
    # IMPORTANT: Replace with actual URLs you intend to scrape (and have permission for)
    target_urls = [
        # "https://www.example-ghana-realestate.com/search?location={}&bedrooms={}".format(
        #     entities.get("location", "accra").replace(" ", "-"),
        #     entities.get("bedrooms", "any")
        # )
        "https://www.meqasa.com/houses-for-rent-in-east-legon", # Example, replace with actual search results page
        # Add more potential URLs or logic to generate them
    ]
    print(f"[Source Discovery] Identified potential URLs: {target_urls}")
    return target_urls

# --- 3. Web Scraping Module ---
def scrape_website_data(url):
    """
    Scrapes property listing data from a given URL.
    This is a conceptual scraper and WILL LIKELY FAIL on real websites
    without specific adaptation to their HTML structure.
    """
    print(f"\n[Web Scraping] Scraping URL: {url}")
    headers = {
        'User-Agent': 'GhanaApartmentDataAgent/1.0 (KHTML, like Gecko; compatible; +http://your-agent-contact-page.com)'
    }
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
    except requests.exceptions.RequestException as e:
        print(f"[Web Scraping] Error fetching {url}: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    listings_data = []

    # --- !!! CRITICAL NOTE !!! ---
    # The following selectors are ENTIRELY HYPOTHETICAL.
    # You MUST inspect the target website's HTML structure and replace these
    # with the correct selectors for that specific site.
    # Websites change their structure often, so scrapers need maintenance.

    # Example: Assume listings are in <div class="property-listing">
    # This will vary greatly from site to site.
    # For meqasa.com, you'd need to inspect its specific structure.
    # For example, a listing might be in an <article class="mqs-prop-card">
    
    # Let's try a hypothetical structure for meqasa based on common patterns (this will likely need adjustment)
    # This is a GUESS and will need to be verified and adjusted by inspecting meqasa.com
    property_cards = soup.find_all('div', class_='mqs-featured-prop-inner-wrap') # This is a guess!
    if not property_cards:
        property_cards = soup.find_all('article', class_='mqs-prop-card') # Another guess!

    print(f"[Web Scraping] Found {len(property_cards)} potential listing elements.")

    for card in property_cards:
        listing = {
            "id": str(uuid.uuid4()), # Generate a unique ID
            "source_url": url,
            "scraped_date_utc": datetime.datetime.utcnow().isoformat(),
            "price_raw": None,
            "location_raw": None,
            "bedrooms_raw": None,
            "description_raw": None,
            "listing_url": None,
            # Add other raw fields as needed
        }

        # Hypothetical selectors - REPLACE THESE
        try:
            # Price (e.g., <span class="price">GHS 5,000</span>) - highly dependent on site
            price_element = card.find('span', class_='h3') # Guess for price on meqasa
            if price_element:
                listing["price_raw"] = price_element.get_text(strip=True)

            # Location (e.g., <p class="location">East Legon, Accra</p>)
            location_element = card.find('address') # Guess for location
            if location_element:
                 listing["location_raw"] = location_element.get_text(strip=True)
            else: # Try another common pattern
                location_element = card.find('p', class_=lambda x: x and 'loc' in x)
                if location_element:
                    listing["location_raw"] = location_element.get_text(strip=True)


            # Bedrooms (e.g., <span class="bedrooms">3 Beds</span>)
            # This often requires more complex parsing or looking for icons/text patterns
            # For meqasa, bedroom info might be in a div with class 'fur-are' or similar
            bedroom_element_container = card.find('div', class_='fur-are')
            if bedroom_element_container:
                # Example: <span title="Bedroom"><i class="fas fa-bed"></i> 3</span>
                bedroom_span = bedroom_element_container.find('span', title=lambda t: t and 'Bedroom' in t)
                if bedroom_span:
                    listing["bedrooms_raw"] = bedroom_span.get_text(strip=True)
            
            # Description (e.g., <p class="description">A beautiful apartment...</p>)
            description_element = card.find('a', class_='mqs-prop-dt-wrapper') # Title often in an 'a' tag
            if description_element:
                listing["description_raw"] = description_element.get('title', description_element.get_text(strip=True))
            
            # Listing URL (often the href of a main link in the card)
            link_element = card.find('a', href=True) # A general link
            if description_element and description_element.has_attr('href'): # Prefer link on title
                 listing["listing_url"] = description_element['href']
            elif link_element:
                 listing["listing_url"] = link_element['href']
            
            # Ensure the URL is absolute
            if listing["listing_url"] and not listing["listing_url"].startswith('http'):
                from urllib.parse import urljoin
                listing["listing_url"] = urljoin(url, listing["listing_url"])


            # Only add if we found some core data, e.g., price or description
            if listing["price_raw"] or listing["description_raw"]:
                listings_data.append(listing)
                print(f"[Web Scraping] Extracted raw listing: Price='{listing['price_raw']}', Loc='{listing['location_raw']}', Beds='{listing['bedrooms_raw']}'")
            else:
                print("[Web Scraping] Card skipped, not enough data found.")


        except Exception as e:
            print(f"[Web Scraping] Error parsing a listing card: {e}")
            # Continue to next card

    if not listings_data:
        print(f"[Web Scraping] No listings extracted. Check HTML structure and selectors for {url}.")
        # You might want to save the raw HTML for debugging:
        # with open(f"debug_page_{url.split('/')[-1]}.html", "w", encoding="utf-8") as f:
        #    f.write(soup.prettify())
        # print(f"[Web Scraping] Saved HTML for debugging: debug_page_{url.split('/')[-1]}.html")


    return listings_data

# --- 4. Data Storage Tool (S3 Bucket Integration) ---
def store_data_s3(data, bucket_name, object_name_prefix):
    """
    Stores data (list of dicts) as a JSON file in an S3 bucket.
    """
    if not data:
        print("[S3 Storage] No data to store.")
        return None

    s3_client = boto3.client('s3', region_name=AWS_REGION)
    # Generate a unique object name, e.g., using date and a UUID
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    object_name = f"{object_name_prefix}/{timestamp}_{str(uuid.uuid4())}.json"

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"[S3 Storage] Successfully stored data in S3: s3://{bucket_name}/{object_name}")
        return f"s3://{bucket_name}/{object_name}"
    except NoCredentialsError:
        print("[S3 Storage] Error: AWS credentials not found. Configure AWS CLI or environment variables.")
    except PartialCredentialsError:
        print("[S3 Storage] Error: Incomplete AWS credentials.")
    except ClientError as e:
        print(f"[S3 Storage] ClientError storing data in S3: {e}")
    except Exception as e:
        print(f"[S3 Storage] An unexpected error occurred: {e}")
    return None

# --- 5. Data Cleaning and Transformation Tool ---
def clean_and_transform_data(raw_listings_data):
    """
    Cleans and transforms raw scraped data.
    """
    print(f"\n[Data Cleaning] Processing {len(raw_listings_data)} raw listings.")
    processed_listings = []
    for raw_listing in raw_listings_data:
        processed = raw_listing.copy() # Start with a copy of all raw fields

        # Price cleaning (example: "GHS 5,000 / month" -> 5000, "GHS", "month")
        if raw_listing.get("price_raw"):
            price_text = raw_listing["price_raw"]
            currency = "GHS" # Assume GHS, can be made more robust
            frequency = "unknown" # e.g., month, year

            if "/ month" in price_text.lower() or "pm" in price_text.lower():
                frequency = "monthly"
            elif "/ year" in price_text.lower() or "pa" in price_text.lower():
                frequency = "yearly"

            # Remove currency symbols, commas, and text
            price_numbers = re.findall(r"[\d\.,]+", price_text)
            if price_numbers:
                try:
                    # Take the first number sequence, remove commas, convert to float
                    cleaned_price = float(price_numbers[0].replace(',', ''))
                    processed["price_numeric"] = cleaned_price
                    processed["price_currency"] = currency
                    processed["price_frequency"] = frequency
                except ValueError:
                    print(f"[Data Cleaning] Could not parse price: {price_text}")
                    processed["price_numeric"] = None # Mark as unparsable

        # Location cleaning (example: "East Legon, Accra" -> "East Legon")
        # This is highly dependent on the format and needs robust parsing
        if raw_listing.get("location_raw"):
            # Simple example: take the part before a comma if present
            processed["location_cleaned"] = raw_listing["location_raw"].split(',')[0].strip()
            # Could add more sophisticated geocoding or normalization here

        # Bedrooms cleaning (example: "3 Beds" -> 3)
        if raw_listing.get("bedrooms_raw"):
            bedroom_match = re.search(r"(\d+)", raw_listing["bedrooms_raw"])
            if bedroom_match:
                try:
                    processed["bedrooms_numeric"] = int(bedroom_match.group(1))
                except ValueError:
                    print(f"[Data Cleaning] Could not parse bedrooms: {raw_listing['bedrooms_raw']}")
                    processed["bedrooms_numeric"] = None

        # Add a flag indicating it's processed
        processed["_is_processed"] = True
        processed["_processed_date_utc"] = datetime.datetime.utcnow().isoformat()

        processed_listings.append(processed)
        # print(f"[Data Cleaning] Processed listing: Price={processed.get('price_numeric')}, Loc='{processed.get('location_cleaned')}', Beds={processed.get('bedrooms_numeric')}")

    print(f"[Data Cleaning] Finished processing. {len(processed_listings)} listings cleaned.")
    return processed_listings

# --- 6. Data Analysis & Retrieval Module (Conceptual) ---
def analyze_and_retrieve_data(processed_data_path_s3, query_entities):
    """
    Retrieves data from S3 (conceptual), filters, and analyzes it.
    In a real system, this might query a database (Athena, Redshift Spectrum)
    or use a data processing framework (Spark, Pandas on a larger scale).
    """
    print(f"\n[Data Analysis] Analyzing data for query: {query_entities}")
    print(f"[Data Analysis] (Conceptual) Assuming data is loaded from {processed_data_path_s3}")

    # This is a placeholder. In reality, you'd download and parse the JSON from S3.
    # For this example, let's assume 'processed_data_path_s3' IS the data if it's passed directly for simplicity.
    # Or, if it's a path, you'd load it:
    # s3 = boto3.client('s3')
    # obj = s3.get_object(Bucket=S3_BUCKET_NAME_PROCESSED, Key=key_from_path)
    # all_processed_listings = json.loads(obj['Body'].read().decode('utf-8'))
    
    # For this conceptual script, we'll assume `processed_data_path_s3` is actually the list of listings
    # if it's not a string (i.e., we are passing data directly for demo)
    if not isinstance(processed_data_path_s3, str):
        all_processed_listings = processed_data_path_s3
    else:
        # Add S3 download logic here if you are actually using S3 paths
        print("[Data Analysis] S3 download and parsing logic would be here.")
        # For now, simulate with an empty list if it's a path
        all_processed_listings = []


    if not all_processed_listings:
        print("[Data Analysis] No processed data available for analysis.")
        return "No data found to analyze."

    # Filter based on query entities
    filtered_listings = []
    for listing in all_processed_listings:
        match = True
        if query_entities.get("location"):
            # Simple substring match, can be improved
            if not listing.get("location_cleaned") or query_entities["location"].lower() not in listing["location_cleaned"].lower():
                match = False
        if query_entities.get("bedrooms"):
            if listing.get("bedrooms_numeric") != query_entities["bedrooms"]:
                match = False
        # Add more filters for property_type, price_range etc.

        if match:
            filtered_listings.append(listing)

    print(f"[Data Analysis] Found {len(filtered_listings)} listings matching criteria.")

    if not filtered_listings:
        return f"No listings found matching your criteria: {query_entities}"

    # Perform analysis (e.g., average price)
    total_price = 0
    min_price = float('inf')
    max_price = float('-inf')
    valid_price_count = 0

    for listing in filtered_listings:
        price = listing.get("price_numeric")
        if price is not None and listing.get("price_frequency") == "monthly": # Assuming we want monthly
            total_price += price
            min_price = min(min_price, price)
            max_price = max(max_price, price)
            valid_price_count += 1
    
    if valid_price_count > 0:
        avg_price = total_price / valid_price_count
        return (f"Found {len(filtered_listings)} listings. "
                f"For {query_entities.get('bedrooms','any')}-bedroom {query_entities.get('property_type','properties')} "
                f"in {query_entities.get('location','Ghana')}: "
                f"Average monthly rent: GHS {avg_price:.2f}. "
                f"Price range: GHS {min_price:.2f} - GHS {max_price:.2f}. "
                f"(Based on {valid_price_count} listings with monthly prices)")
    else:
        return f"Found {len(filtered_listings)} listings, but none had parseable monthly pricing information matching your criteria."


# --- 7. Response Generation Module (Integrated into Analysis for this example) ---
# The analyze_and_retrieve_data function currently generates the response string.

# --- Main Agent Orchestration (Conceptual) ---
def run_agent(user_query):
    """
    Main orchestration logic for the agent.
    """
    print(f"--- Running Ghana Apartment Data Agent for query: '{user_query}' ---")

    # 1. Understand Query
    entities = understand_query(user_query)
    if not entities.get("location") and not entities.get("bedrooms"): # Basic check
        print("Agent: Could not understand key details from your query (e.g., location, bedrooms).")
        return

    # 2. Discover Sources (In a real agent, this might be more dynamic)
    source_urls = discover_sources(entities)
    if not source_urls:
        print("Agent: Could not identify any sources to scrape for your query.")
        return

    # 3. Scrape Data from sources
    all_raw_data = []
    for url in source_urls:
        # IMPORTANT: Ethical scraping - respect robots.txt, terms of service, rate limits.
        # Add delays between requests: time.sleep(5)
        raw_data_from_url = scrape_website_data(url)
        if raw_data_from_url:
            all_raw_data.extend(raw_data_from_url)
    
    if not all_raw_data:
        print("Agent: No data was scraped from the identified sources.")
        # Potentially try alternative sources or inform user.
        # For now, we'll stop if no raw data.
        # In a more robust system, you might fall back to older cached data or broaden search.
        # For this example, we'll try to analyze any cached processed data if scraping fails.
        # This part needs more thought for a real application.
        print("Agent: Attempting to analyze previously processed data if available.")
        # This assumes you have a way to point to existing processed data.
        # For this demo, we'll just skip to a message.
        # A real system would query a manifest of processed S3 files.
        # For now, let's simulate having NO data if scraping yields nothing.
        # To actually use cached data, you'd need to list S3 processed files and load one.
        # For this example, we will proceed with empty data if scraping fails.
        # This means analysis will likely report no data.

    # 4. Store Raw Data (Optional, but good for reprocessing)
    if all_raw_data:
        raw_s3_path = store_data_s3(all_raw_data, S3_BUCKET_NAME_RAW, "raw_listings")
        if raw_s3_path:
            print(f"Agent: Raw data stored at {raw_s3_path}")
        else:
            print("Agent: Failed to store raw data in S3.")
    else:
        print("Agent: No new raw data to store.")


    # 5. Clean and Transform Data
    # In a real pipeline, you might fetch all recent raw data or specific files.
    # For this example, we use the just-scraped data.
    processed_listings = clean_and_transform_data(all_raw_data)

    # 6. Store Processed Data
    processed_s3_path = None
    if processed_listings:
        processed_s3_path = store_data_s3(processed_listings, S3_BUCKET_NAME_PROCESSED, "processed_listings")
        if processed_s3_path:
            print(f"Agent: Processed data stored at {processed_s3_path}")
        else:
            print("Agent: Failed to store processed data in S3.")
    else:
        print("Agent: No data was processed (either no raw data or cleaning yielded nothing).")


    # 7. Analyze Data and Generate Response
    # If new data was processed and stored, use its path.
    # Otherwise, conceptually, you might try to load the LATEST processed data from S3.
    # For this simplified example, we'll use the `processed_listings` directly if available.
    # A more robust solution would query S3 for the latest file in S3_BUCKET_NAME_PROCESSED.
    
    final_response = ""
    if processed_listings: # Prefer to analyze freshly processed data
        print("Agent: Analyzing freshly scraped and processed data.")
        final_response = analyze_and_retrieve_data(processed_listings, entities)
    else:
        # Conceptual: Try to load latest from S3_BUCKET_NAME_PROCESSED
        print("Agent: No fresh data. (Conceptual: Would attempt to load latest processed data from S3 here).")
        # For this example, we'll just state no data.
        final_response = "Agent: No new data was processed, and loading historical data is not implemented in this example."
        # To implement loading:
        # 1. List objects in S3_BUCKET_NAME_PROCESSED.
        # 2. Find the latest file.
        # 3. Download and parse it.
        # 4. Pass it to analyze_and_retrieve_data.
        # Example (very basic S3 list and get):
        # try:
        #    s3_res = boto3.resource('s3')
        #    bucket = s3_res.Bucket(S3_BUCKET_NAME_PROCESSED)
        #    latest_file = None
        #    latest_mod = None
        #    for obj_summary in bucket.objects.filter(Prefix="processed_listings/"):
        #        if latest_mod is None or obj_summary.last_modified > latest_mod:
        #            latest_mod = obj_summary.last_modified
        #            latest_file = obj_summary.key
        #    if latest_file:
        #        print(f"Agent: Found latest processed file: {latest_file}")
        #        s3_client = boto3.client('s3')
        #        response_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME_PROCESSED, Key=latest_file)
        #        historical_data = json.loads(response_obj['Body'].read().decode('utf-8'))
        #        final_response = analyze_and_retrieve_data(historical_data, entities)
        #    else:
        #        final_response = "Agent: No fresh data and no historical processed data found in S3."
        # except Exception as e:
        #    print(f"Agent: Error trying to load historical data from S3: {e}")
        #    final_response = "Agent: Error accessing historical data."


    print(f"\n--- Agent Response ---")
    print(final_response)
    print("--- Agent Run Complete ---")


if __name__ == '__main__':
    # --- Pre-run Checks & Setup ---
    # AWS Credentials: Ensure your environment is configured for Boto3.
    # This can be via AWS CLI (`aws configure`), IAM roles (if on EC2/Lambda), or env vars.
    print("Reminder: Ensure AWS credentials and region are configured for S3 interaction.")
    print(f"Using S3 Raw Bucket: {S3_BUCKET_NAME_RAW}")
    print(f"Using S3 Processed Bucket: {S3_BUCKET_NAME_PROCESSED}")
    print("---")


    run_agent()
    # Example User Query
    # query = "How much does rent cost in East Legon for a 4-bedroom apartment?"
    query1 = "show me rent for 2 bedroom apartment in Osu"
    query2 = "4 bed house in Cantonments price"
    query3 = "what is the average rent for a 1 bedroom in Airport Residential" # scraper might not find 1 bed

    # Run the agent with a sample query
    # Important: The scraper is generic and will likely need adjustments for any specific website.
    # The meqasa URL in discover_sources is an example; you'd need to ensure it's a valid search results page
    # or adapt the scraper to navigate to one.
    # run_agent(query1)

    # You can test with other queries:
    # run_agent(query2)
    # run_agent(query3)

    # --- Notes for Development ---
    # 1. Scraper Robustness: Web scraping is fragile. Websites change.
    #    - Use specific, stable selectors.
    #    - Implement error handling for each field extraction.
    #    - Consider tools like Scrapy for larger projects.
    #    - HEADLESS BROWSERS (e.g., Selenium, Playwright) might be needed for JavaScript-heavy sites,
    #      but they add complexity and resource overhead.
    # 2. NLP for Query Understanding: For better understanding, use spaCy, NLTK, or an LLM API.
    # 3. Data Storage:
    #    - For processed data, consider more structured storage if querying often (e.g., RDS, DynamoDB, or Athena on S3).
    #    - Parquet or ORC format in S3 is good for analytics.
    # 4. Scheduling: Use cron, AWS Lambda scheduled events, or Airflow to run scraping periodically.
    # 5. Ethical Scraping:
    #    - Always check `robots.txt`.
    #    - Respect website Terms of Service.
    #    - Don't overload servers (use delays, scrape during off-peak hours if possible).
    #    - Set a clear User-Agent.
    # 6. Configuration Management: Move bucket names, AWS region, etc., to config files or env vars.
    # 7. Error Handling & Logging: Implement comprehensive logging and error handling throughout.
