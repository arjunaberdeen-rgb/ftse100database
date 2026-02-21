import yfinance as yf
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

def get_ftse100_tickers():
    """Scrapes FTSE 100 tickers from Wikipedia using BeautifulSoup."""
    url = 'https://en.wikipedia.org/wiki/FTSE_100_Index'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table', {'class': 'wikitable'})
    tickers = []
    print(f"Found {len(tables)} wikitable tables on Wikipedia.")
    for idx, table in enumerate(tables):
        header = [th.text.strip() for th in table.find_all('th')]
        print(f"Table {idx} headers: {header}")
        # Try to find the column for ticker (EPIC or Ticker)
        epic_idx = None
        for i, col in enumerate(header):
            if 'EPIC' in col or 'Ticker' in col:
                epic_idx = i
                break
        if epic_idx is not None:
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) > epic_idx:
                    epic = cols[epic_idx].text.strip()
                    if epic:
                        tickers.append(epic + '.L')
            break
    print(f"Scraped tickers: {tickers}")
    return tickers

def download_ftse_data(start_date="2020-01-01", end_date="2026-01-01"):
    """Downloads historical data for FTSE 100 companies in batches."""
    print("Fetching FTSE 100 tickers...")
    tickers = get_ftse100_tickers()
    print(f"Scraped tickers: {tickers}")
    if not tickers:
        print("No tickers scraped from Wikipedia. Check the table structure or scraping logic.")
        return
    print(f"Downloading data for {len(tickers)} companies from {start_date} to {end_date}...")
    # Download in batches of 10 to avoid timeout
    all_data = []
    batch_size = 10
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"Downloading batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1}: {batch}")
        try:
            data = yf.download(batch, start=start_date, end=end_date, group_by='ticker', progress=False)
            if data.empty:
                print(f"No data returned for batch: {batch}")
            else:
                all_data.append(data)
        except Exception as e:
            print(f"Error downloading batch {batch}: {e}")
            continue
    if all_data:
        # Combine all batches
        combined_data = pd.concat(all_data, axis=1)
        output_file = "ftse100_database.csv"
        combined_data.to_csv(output_file)
        print(f"Data saved to {output_file}")
    else:
        print("No data was downloaded successfully. Check ticker format or Yahoo Finance availability.")

if __name__ == "__main__":
    download_ftse_data()
