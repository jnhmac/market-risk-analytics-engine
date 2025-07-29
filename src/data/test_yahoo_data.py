import yfinance as yf

# Get the same AAPL data for 2025-07-25
aapl = yf.download('AAPL', start='2025-07-25', end='2025-07-26')
print("Yahoo Finance AAPL data for 2025-07-25:")
print(aapl)
print("\nColumn names:", aapl.columns.tolist())
print("Data types:", aapl.dtypes)
print("\nIndex type:", type(aapl.index[0]))