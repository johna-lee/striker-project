import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_fbref_tables(url):
    # Add headers to avoid being blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Make the request
    response = requests.get(url, headers=headers)
    
    # Check if request was successful
    if response.status_code != 200:
        print(f"Failed to retrieve page: Status code {response.status_code}")
        return None
    
    # Parse HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    
    # Dictionary to store all table data
    all_tables = {}
    
    # Process each table
    for i, table in enumerate(tables):
        # Get table header
        thead = table.find('thead')
        tbody = table.find('tbody')
        
        # Skip if table doesn't have both thead and tbody
        if not thead or not tbody:
            continue
            
        # Get table ID or create a generic name
        table_id = table.get('id', f'table_{i+1}')
        
        # Extract header rows
        header_rows = []
        for tr in thead.find_all('tr'):
            header_row = []
            for th in tr.find_all(['th', 'td']):
                # Get colspan value (default to 1 if not specified)
                colspan = int(th.get('colspan', 1))
                # Add cell content to row (repeated if colspan > 1)
                header_row.extend([th.text.strip()] * colspan)
            header_rows.append(header_row)
            
        # Extract body rows
        body_rows = []
        for tr in tbody.find_all('tr'):
            body_row = []
            for td in tr.find_all(['td', 'th']):
                # Handle colspan similar to headers
                colspan = int(td.get('colspan', 1))
                body_row.extend([td.text.strip()] * colspan)
            body_rows.append(body_row)
            
        # Store the data
        all_tables[table_id] = {
            'header_rows': header_rows,
            'body_rows': body_rows
        }
    
    return all_tables

def save_tables_to_csv(all_tables):
    for table_id, table_data in all_tables.items():
        # If there are multiple header rows, use the last one as column names
        header = table_data['header_rows'][-1] if table_data['header_rows'] else []
        
        # Create DataFrame
        df = pd.DataFrame(table_data['body_rows'], columns=header if header else None)
        
        # Save to CSV
        filename = f"{table_id}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved table {table_id} to {filename}")

# URL of the match
url = "https://fbref.com/en/matches/07f058d4/Dinamo-Zagreb-Chelsea-September-6-2022-Champions-League"

# Scrape tables
print("Scraping tables from FBref...")
all_tables = scrape_fbref_tables(url)

if all_tables:
    print(f"Found {len(all_tables)} tables with thead and tbody elements.")
    
    # Print the table IDs
    print("Table IDs:")
    for table_id in all_tables.keys():
        print(f" - {table_id}")
    
    # Save tables to CSV
    save_tables_to_csv(all_tables)
else:
    print("No tables found or scraping failed.")