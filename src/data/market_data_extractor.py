import requests
import os
from dotenv import load_dotenv
from ..config.portfolio import AI_PORTFOLIO, ALL_SYMBOLS, API_CONFIG

load_dotenv()

def fetch_market_data(symbol: str, api_key: str, outputsize: str = "compact", datatype: str = "json") -> dict:
   """
   Fetch daily market data from Alpha Vantage API connection for TIME_SERIES_DAILY.

   Args:
       symbol (str): Stock ticker symbol (e.g., 'NVDA', "MSFT).
       api_key (str): Alpha Vantage API key.
       outputsize (str): Either 'compact' (last 100 data points) or 'full' (20+ years).
       datatype (str): Either 'json' or 'csv'.

   Returns:
    dict: JSON response with time series data or error information.
    
   Example:
    >>> data = fetch_market_data("NVDA", api_key)
    >>> latest_date = next(iter(data["Time Series (Daily)"]))
    >>> print(data["Time Series (Daily)"][latest_date]["4. close"])
   """

   url = "https://www.alphavantage.co/query"
   params = {
       "function": "TIME_SERIES_DAILY",
       "symbol": symbol,
       "outputsize": outputsize,
       "datatype": datatype,
       "apikey": api_key
   }

   try:
       response = requests.get(url, params=params, timeout=10)
       response.raise_for_status()
       if datatype == "json":
           return response.json()
       return {"content": response.text}
   except requests.RequestException as e:
       return {"error": str(e)}

# Main execution  
if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    if not API_KEY:
        print("Error: ALPHA_VANTAGE_API_KEY not found in environment variables")
        exit(1)
    
    # Test single stock first
    print("--- Testing IBM ---")
    result = fetch_market_data("IBM", API_KEY)
    
    if "error" in result:
        print("API Request Failed:", result["error"])
    elif "Time Series (Daily)" in result:
        latest_date = next(iter(result["Time Series (Daily)"]))
        print(f"Latest data on {latest_date}:")
        print(result["Time Series (Daily)"][latest_date])
    else:
        print("Unexpected response:", result)
    
    # Process AI portfolio using config
    for stock in ALL_SYMBOLS:
        print(f"\n--- Processing {stock} ---")
        # NOTE: Using 'compact' for initial development (100 data points)
        # TODO: Switch to outputsize="full" in Week 2 for VaR calculations (20+ years of data)
        result = fetch_market_data(stock, API_KEY)
        
        if "error" in result:
            print(f"API Request Failed for {stock}:", result["error"])
        elif "Time Series (Daily)" in result:
            latest_date = next(iter(result["Time Series (Daily)"]))
            print(f"Latest data for {stock} on {latest_date}:")
            print(result["Time Series (Daily)"][latest_date])
        else:
            print(f"Unexpected response for {stock}:", result)