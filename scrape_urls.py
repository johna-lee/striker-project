import httpx
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# Set up headers to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.google.com/',
    'sec-ch-ua': '"Google Chrome";v="121", " Not;A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

# URL to scrape
url = "https://fbref.com/en/comps/8/schedule/Champions-League-Scores-and-Fixtures"

try:
    # Delay to avoid rate limiting
    time.sleep(5)
    
    # Create a client session with persistent cookies
    with httpx.Client(headers=headers, follow_redirects=True, timeout=30) as client:
        # Make a request to the website
        print(f"Sending request to {url}...")
        response = client.get(url)
        
        # Check if request was successful
        if response.status_code == 200:
            print("Successfully connected to the website!")
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the main schedule table
            table = soup.find('table', id='sched_all')
            
            if table:
                # Initialize a list to store the match report URLs
                match_report_urls = []
                
                # Find all rows in the table
                rows = table.find_all('tr')
                
                # Loop through each row
                for row in rows:
                    # Look for the Match Report link
                    match_report_cell = row.find('td', {'data-stat': 'match_report'})
                    
                    if match_report_cell and match_report_cell.find('a'):
                        # Extract the href attribute
                        match_report_link = match_report_cell.find('a')['href']
                        
                        # Construct the full URL
                        full_url = f"https://fbref.com{match_report_link}"
                        
                        # Add to our list
                        match_report_urls.append(full_url)
                
                # Print the results
                print(f"Found {len(match_report_urls)} match report URLs:")
                for url in match_report_urls:
                    print(url)
                
                # Save to CSV file
                df = pd.DataFrame({'Match Report URL': match_report_urls})
                df.to_csv('champions_league_match_reports.csv', index=False)
                print("\nURLs have been saved to 'champions_league_match_reports.csv'")
            else:
                print("Could not find the schedule table on the page.")
        else:
            print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
            print("Response:", response.text[:500])

except Exception as e:
    print(f"An error occurred: {e}")