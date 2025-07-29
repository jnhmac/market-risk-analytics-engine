"""
Bronze Layer: Raw data ingestion for Market Risk Analytics Engine
Stores immutable raw data from multiple sources
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Optional

class BronzeDataIngestion:
    def __init__(self, data_path: str = "data/bronze"):
        self.data_path = data_path
        self.create_directories()
    
    def create_directories(self):
        """Create bronze layer directory structure"""
        # Create the main bronze directory
        os.makedirs(self.data_path, exist_ok=True)
        
        # Create subdirectories for each data source
        yahoo_path = os.path.join(self.data_path, "yahoo_finance")
        alpha_path = os.path.join(self.data_path, "alpha_vantage")
        
        os.makedirs(yahoo_path, exist_ok=True)
        os.makedirs(alpha_path, exist_ok=True)
        
        print(f"Created directories:")
        print(f"  - {self.data_path}")
        print(f"  - {yahoo_path}")
        print(f"  - {alpha_path}")
    
    def ingest_yahoo_finance_data(self, symbols: List[str], start_date: str, end_date: str) -> bool:
        """
        Ingest raw Yahoo Finance data and store in bronze layer
        
        Args:
            symbols: List of stock symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            bool: Success status
        """
        try:
            for symbol in symbols:
                print(f"Downloading {symbol} data...")
                
                # Download data using yfinance
                data = yf.download(symbol, start=start_date, end=end_date)
                
                # Create filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{symbol}_{start_date}_{end_date}_{timestamp}.csv"
                filepath = os.path.join(self.data_path, "yahoo_finance", filename)
                
                # Save raw data to CSV
                data.to_csv(filepath)
                print(f"Saved {symbol} data to {filepath}")
                
            return True
            
        except Exception as e:
            print(f"Error ingesting Yahoo Finance data: {e}")
            return False
    
    def ingest_alpha_vantage_data(self, symbol: str) -> bool:
        """
        Ingest raw Alpha Vantage data and store in bronze layer
        
        Args:
            symbol: Stock symbol
            
        Returns:
            bool: Success status
        """
        # TODO: Implement Alpha Vantage data ingestion
        pass

if __name__ == "__main__":
    bronze = BronzeDataIngestion()
    
    # Test with 3 AI stocks for 1 week
    test_symbols = ['AAPL', 'NVDA', 'MSFT','PLTR']
    success = bronze.ingest_yahoo_finance_data(
        symbols=test_symbols,
        start_date='2025-07-20', 
        end_date='2025-07-25'
    )
    
    if success:
        print("✅ Yahoo Finance ingestion successful!")
    else:
        print("❌ Yahoo Finance ingestion failed!")