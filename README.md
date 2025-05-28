# Ghana Rental Agent

An AI-powered agent that scrapes real estate data from Ghanaian property websites, processes it, and allows users to query rental information using natural language through Google ADK.

## Features

- Real-time web scraping of Ghanaian rental properties
- Natural language query processing
- Price analysis and statistics
- Support for multiple property types (apartments, houses, townhouses)
- Location-based search across major Ghanaian cities
- Currency conversion (USD to GHS)
- Automated data cleaning and processing

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/amoako419/Rental_Agent.git
    cd Rental_Agent
    ```

2. Create and activate a virtual environment:
    ```bash
    python -m venv .venv
    .venv\Scripts\activate
    ```

3. Install required packages:
    ```bash
    pip install -r requirements.txt
    ```

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
# AWS Configuration (optional)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_SESSION_TOKEN=optional_session_token

# S3 Bucket Names (optional)
S3_BUCKET_NAME_RAW=ghana-apartments-raw-data
S3_BUCKET_NAME_PROCESSED=ghana-apartments-processed-data

# Currency Configuration
GHS_USD_EXCHANGE_RATE=14.5
```

## Usage

Run the agent from the command line:

```bash
python rental_agent/agent.py
```

Example queries:
- "How much does rent cost in East Legon for a 4-bedroom apartment?"
- "Show me rent for 2 bedroom apartment in Osu"
- "4 bed house in Cantonments price"
- "What is the average rent for a 1 bedroom in Airport Residential?"

## Supported Locations

The agent supports queries for various locations in Ghana, including:
- East Legon
- Cantonments
- Osu
- Airport Residential Area
- Airport Hills
- Labone
- Roman Ridge
- Downtown Accra
- Spintex
- Tema
- Kumasi
- Takoradi
- And more...

## Data Sources

Currently scrapes data from:
- Meqasa.com (Ghana's leading real estate marketplace)

## Technical Details

- Built with Python 3.x
- Uses Google ADK for natural language processing
- BeautifulSoup4 for web scraping
- Optional AWS S3 integration for data storage
- Regular expressions for data cleaning
- Supports both GHS and USD pricing

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Note

This tool is for educational and research purposes only. Please respect the terms of service and robots.txt files of the websites being scraped.
