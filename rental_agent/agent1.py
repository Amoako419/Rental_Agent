from urllib.parse import urljoin
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import google_search
from google.genai import types
import re
import uuid
import requests
import datetime
from bs4 import BeautifulSoup
import time


APP_NAME="google_search_agent"
USER_ID="user1234"
SESSION_ID="1234"

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
    return all_raw_data


root_agent = Agent(
    name="ghana_rental_analyzer",
    model="gemini-2.0-flash",
    description=(
        "Provides information about apartment and house rentals in Ghana, "
        "including average prices, price ranges, and availability based on location and number of bedrooms. "
        "It scrapes data from Ghanaian real estate websites."
    ),
    instruction=(
        "You are an expert real estate agent for Ghana. "
        "Use the `get_ghana_apartment_data` tool to find rental properties based on the user's query. "
        "After using the tool, provide a concise summary of the findings from the tool's report, "
        "including rental prices, price ranges, and availability based on location and number of bedrooms. "
        "If the tool returns an error or no data, inform the user clearly. "
        "Always state prices in GHS (Ghanaian Cedis). If you need to search for general information not related to property data, use the `Google Search` tool."
    ),
    tools=[google_search],

)
# Session and Runner
session_service = InMemorySessionService()
session = session_service.create_session(app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID)
runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)


# Agent Interaction
def call_agent(query):
    """
    Helper function to call the agent with a query.
    """
    content = types.Content(role='user', parts=[types.Part(text=query)])
    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            print("Agent Response: ", final_response)