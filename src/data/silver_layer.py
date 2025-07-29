"""
Silver Layer: Clean and standardize data from Bronze layer
"""

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict

class SilverDataProcessor:
    def __init__(self, bronze_path: str = "data/bronze", silver_path: str = "data/silver"):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.create_directories()
    
    def create_directories(self):
        """Create silver layer directory"""
        os.makedirs(self.silver_path, exist_ok=True)
        print(f"Created silver layer directory: {self.silver_path}")
    
    def process_yahoo_finance_file(self, bronze_file_path: str) -> pd.DataFrame:
        """
        Transform one Bronze Yahoo Finance file into clean Silver format
        
        Args:
            bronze_file_path: Path to bronze CSV file
            
        Returns:
            Clean DataFrame with standardized schema and metadata
        """
        # Read Bronze file, skip the messy header rows
        raw_data = pd.read_csv(bronze_file_path, skiprows=2)
        
        # Assign proper column names
        raw_data.columns = ['date', 'close', 'high', 'low', 'open', 'volume']
        
        # Extract symbol from filename
        filename = os.path.basename(bronze_file_path)
        symbol = filename.split('_')[0]
        
        # Add metadata columns
        raw_data['symbol'] = symbol
        raw_data['source'] = 'yahoo_finance'
        raw_data['ingestion_timestamp'] = datetime.now()
        
        return raw_data
    
    def process_all_bronze_files(self) -> List[str]:
        """Process all Bronze Yahoo Finance files to Silver layer"""
        yahoo_bronze_path = os.path.join(self.bronze_path, "yahoo_finance")
        silver_files = []
        
        # Debug: Check path
        print(f"Looking in: {yahoo_bronze_path}")
        print(f"Path exists: {os.path.exists(yahoo_bronze_path)}")
        
        # Get all Bronze CSV files
        bronze_files = [f for f in os.listdir(yahoo_bronze_path) if f.endswith('.csv')]
        print(f"Found {len(bronze_files)} Bronze files to process:")
        
        # List all files found
        for i, bronze_file in enumerate(bronze_files, 1):
            print(f"  {i}. {bronze_file}")
        
        # Process each file
        for bronze_file in bronze_files:
            print(f"\n{'='*30}")
            print(f"Processing: {bronze_file}")
            bronze_file_path = os.path.join(yahoo_bronze_path, bronze_file)
            
            # Transform Bronze → Silver
            clean_data = self.process_yahoo_finance_file(bronze_file_path)
            
            # Extract symbol for filename
            symbol = bronze_file.split('_')[0]
            
            # Save to Silver
            silver_file = self.save_to_silver(clean_data, symbol)
            silver_files.append(silver_file)
        
        print(f"\n✅ Successfully processed {len(silver_files)} files to Silver layer")
        return silver_files
       
    def save_to_silver(self, clean_data: pd.DataFrame, symbol: str) -> str:
        """Save clean DataFrame to Silver layer"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{symbol}_silver_{timestamp}.csv"
        filepath = os.path.join(self.silver_path, filename)
        
        clean_data.to_csv(filepath, index=False)
        print(f"Saved Silver data to: {filepath}")
        return filepath
       

if __name__ == "__main__":
    silver = SilverDataProcessor()
    print("Silver layer setup complete!")
    
    # Process all Bronze files
    print("\n" + "="*50)
    print("Processing ALL Bronze files:")
    silver_files = silver.process_all_bronze_files()