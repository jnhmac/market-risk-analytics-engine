"""
Gold Layer: Business metrics and analytics-ready data
"""

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict
import glob

class GoldDataProcessor:
    def __init__(self, silver_path: str = "data/silver", gold_path: str = "data/gold"):
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.create_directories()
    
    def create_directories(self):
        """Create gold layer directory"""
        os.makedirs(self.gold_path, exist_ok=True)
        print(f"Created gold layer directory: {self.gold_path}")    
    
    
    def load_all_silver_data(self) -> pd.DataFrame:
        """Load and combine all Silver files into single DataFrame"""
        silver_files = glob.glob(os.path.join(self.silver_path, "*_silver_*.csv"))
        
        print(f"Found {len(silver_files)} Silver files:")
        for file in silver_files:
            print(f"  - {os.path.basename(file)}")
        
        # Read and combine all Silver files
        all_data = []
        for file in silver_files:
            df = pd.read_csv(file)
            all_data.append(df)
        
        # Combine into single DataFrame
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Convert date column to datetime
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        
        print(f"\nCombined data shape: {combined_df.shape}")
        print(f"Symbols: {sorted(combined_df['symbol'].unique())}")
        print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        
        return combined_df
    
    def calculate_daily_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate daily returns for each stock"""
        # Sort by symbol and date to ensure proper order
        df_sorted = df.sort_values(['symbol', 'date']).copy()
        
        # Calculate daily returns for each stock
        df_sorted['daily_return'] = df_sorted.groupby('symbol')['close'].pct_change()
        
        # Calculate additional metrics
        df_sorted['price_change'] = df_sorted.groupby('symbol')['close'].diff()
        df_sorted['volume_ma_3d'] = df_sorted.groupby('symbol')['volume'].rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)
        
        print("Added business metrics:")
        print("- daily_return: Daily percentage change in closing price")
        print("- price_change: Daily dollar change in closing price") 
        print("- volume_ma_3d: 3-day moving average of volume")
        
        return df_sorted
    
    def save_gold_data(self, df: pd.DataFrame, filename: str = "portfolio_metrics") -> str:
        """Save Gold data to CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.gold_path, f"{filename}_{timestamp}.csv")
        
        df.to_csv(filepath, index=False)
        print(f"Saved Gold data to: {filepath}")
        return filepath


if __name__ == "__main__":
    gold = GoldDataProcessor()
    print("Gold layer setup complete!")
    
    # Load all Silver data
    print("\n" + "="*50)
    print("Loading Silver data:")
    combined_data = gold.load_all_silver_data()


    # Calculate business metrics
    print("\n" + "-"*30)
    print("Calculating business metrics:")
    gold_data = gold.calculate_daily_returns(combined_data)
    print(f"Gold data shape: {gold_data.shape}")

        # Save Gold data
    print("\n" + "-"*30)
    gold_file = gold.save_gold_data(gold_data)
    
    # Preview the metrics
    print("\nSample of Gold data with business metrics:")
    print(gold_data[['date', 'symbol', 'close', 'daily_return', 'price_change']].head(8))