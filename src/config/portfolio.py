"""
Portfolio configuration for AI Market Risk Analytics
"""

# AI Portfolio stocks by tier
AI_PORTFOLIO = {
    "tier_1": ["NVDA", "MSFT", "GOOGL", "AMZN", "META", "AAPL"],
    "tier_2": ["AMD", "CRM", "ORCL"], 
    "tier_3": ["PLTR", "AI", "SNOW", "MDB", "SMCI"],
    "benchmark": ["BOTZ"]
}

# Flatten to single list for API calls
ALL_SYMBOLS = [symbol for tier in AI_PORTFOLIO.values() for symbol in tier]

# API Configuration
API_CONFIG = {
    "base_url": "https://www.alphavantage.co/query",
    "function": "TIME_SERIES_DAILY",
    "timeout": 10
}