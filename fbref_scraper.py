import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import os
from google.cloud import storage

# Extract the match ID from the URL
def extract_match_id(url):
    match = re.search(r'/matches/([^/]+)/', url)
    if match:
        return match.group(1)
    return None

# Scrape only tables with target key columns
def scrape_specific_tables(url):
    key_columns = ["Player", "Min", "Gls", "Ast", "xG", "Pos"]
    
    # Add headers to avoid being blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Make the request
    try:
        response = requests.get(url, headers=headers)
        
        # Check if request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve page: Status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    
    # Parse HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    
    # Dictionary to store matching tables
    matching_tables = {}
    table_count = 0
    
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
        
        # If there are multiple header rows, use the last one
        if not header_rows:
            continue
            
        last_header = header_rows[-1]
        
        # Check if this header row matches our key column headers
        if all(col in last_header for col in key_columns):
            print(f"Found matching table: {table_id}")
            
            # Extract body rows
            body_rows = []
            for tr in tbody.find_all('tr'):
                # Skip header rows in body
                if tr.get('class') and 'thead' in tr.get('class'):
                    continue
                
                # Skip summary rows
                if tr.get('class') and 'sum' in tr.get('class'):
                    continue
                    
                body_row = []
                for td in tr.find_all(['td', 'th']):
                    # Handle colspan similar to headers
                    colspan = int(td.get('colspan', 1))
                    body_row.extend([td.text.strip()] * colspan)
                
                # Only add rows with content
                if body_row and any(cell.strip() for cell in body_row):
                    # Make sure the row has the correct length
                    if len(body_row) == len(last_header):
                        body_rows.append(body_row)
            
            # Store the data if we have rows
            if body_rows:
                # Get team name from caption or from table ID
                team_name = "Unknown"
                caption = table.find('caption')
                if caption:
                    caption_text = caption.text.strip()
                    # Extract team name from caption (usually in the format "Team_Name Player Statistics")
                    if " Player " in caption_text:
                        team_name = caption_text.split(" Player ")[0].strip()
                
                # Use a unique key for this table
                table_count += 1
                key = f"team_{table_count}"
                
                matching_tables[key] = {
                    'team_name': team_name,
                    'header': last_header,
                    'body_rows': body_rows
                }
    
    return matching_tables

# Process the matching tables and combine them into a single DataFrame
def process_and_combine_tables(tables, match_id):
    all_dataframes = []
    
    # Create DataFrame
    for key, table_data in tables.items():
        df = pd.DataFrame(table_data['body_rows'], columns=table_data['header'])
        
        # Add match_id column to each row
        df.insert(0, 'match_id', match_id)
        
        # Add team name column
        df.insert(1, 'team', table_data['team_name'])
        
        # Remove country flag from Nation column
        if 'Nation' in df.columns:
            df['Nation'] = df['Nation'].apply(lambda x: x.split(' ', 1)[1] if ' ' in x else x)
        
        # Add to list of DataFrames
        all_dataframes.append(df)
        print(f"Processed {key} ({table_data['team_name']}) with {len(df)} rows")
    
    # Combine all DataFrames into one
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df
    else:
        return None

# Upload file to GCS
def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        
        # Get bucket
        bucket = storage_client.get_bucket(bucket_name)
        
        # Create blob
        blob = bucket.blob(destination_blob_name)
        
        # Upload file
        blob.upload_from_filename(local_file_path)
        
        print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return False

# Process single match URL, save results locally, upload to GCS
def process_match(url, output_dir=".", bucket_name=None):
    print(f"\n{'='*80}\nProcessing: {url}\n{'='*80}")
    
    # Extract match ID from URL
    match_id = extract_match_id(url)
    if not match_id:
        print("Could not extract match ID from URL")
        return {'url': url, 'match_id': None, 'status': 'Failed', 'reason': 'Invalid URL format'}
    
    print(f"Match ID: {match_id}")
    
    # Scrape specific tables from the page
    matching_tables = scrape_specific_tables(url)
    
    if not matching_tables:
        print("No matching tables found")
        return {'url': url, 'match_id': match_id, 'status': 'Failed', 'reason': 'No matching tables found'}
    
    print(f"Found {len(matching_tables)} matching tables")
    
    # Process and combine tables
    combined_df = process_and_combine_tables(matching_tables, match_id)
    
    if combined_df is None:
        print("No data to save")
        return {'url': url, 'match_id': match_id, 'status': 'Failed', 'reason': 'No data to process'}
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save combined DataFrame to CSV
    file_name = f"match_{match_id}_player_stats.csv"
    output_file = os.path.join(output_dir, file_name)
    combined_df.to_csv(output_file, index=False)
    print(f"Saved combined player stats to: {output_file}")
    
    # Result dictionary with default values
    result = {
        'url': url, 
        'match_id': match_id, 
        'status': 'Success', 
        'rows': len(combined_df),
        'tables': len(matching_tables),
        'output_file': output_file,
        'gcs_upload': 'Not attempted'
    }
    
    # Upload to GCS
    if bucket_name:
        gcs_path = f"match_data/{file_name}"
        upload_success = upload_to_gcs(output_file, bucket_name, gcs_path)
        result['gcs_upload'] = 'Success' if upload_success else 'Failed'
        result['gcs_path'] = f"gs://{bucket_name}/{gcs_path}" if upload_success else None
    
    return result

def main():
    # Input file with URLs
    input_file = "fbref_urls.csv"
    
    # Output directory for CSV files
    output_dir = "match_data"
    
    # Output file for the processing report
    report_file = "processing_report.csv"
    
    # GCS bucket name
    bucket_name = "striker-project"
    
    print(f"Google Cloud Storage bucket: {bucket_name if bucket_name else 'Not specified (upload disabled)'}")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found")
        return
    
    # Read URLs from CSV
    try:
        # Try to read URLs from the CSV file
        urls_df = pd.read_csv(input_file)
        
        # Define URLs column
        url_column = urls_df.columns[0]
        
        # Extract URLs
        urls = urls_df[url_column].tolist()
        
        if not urls:
            print("No URLs found in the CSV file")
            return
        
        print(f"Found {len(urls)} URLs to process")
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    # Process each URL
    results = []
    
    for i, url in enumerate(urls):
        print(f"\nProcessing URL {i+1}/{len(urls)}")
        
        try:
            result = process_match(url, output_dir, bucket_name)
            results.append(result)
            
            # Add a delay between requests to avoid rate limiting
            if i < len(urls) - 1:  # No delay after the last URL
                delay = 3  # 3 second delay
                print(f"Waiting {delay} seconds before next request...")
                time.sleep(delay)
                
        except Exception as e:
            print(f"Error processing URL: {e}")
            results.append({
                'url': url,
                'match_id': extract_match_id(url),
                'status': 'Error',
                'reason': str(e),
                'gcs_upload': 'Not attempted'
            })
    
    # Create processing report
    report_df = pd.DataFrame(results)
    report_df.to_csv(report_file, index=False)
    print(f"\nProcessing report saved to {report_file}")
    
    # Upload report to GCS
    if bucket_name:
        upload_to_gcs(report_file, bucket_name, f"reports/{report_file}")
    
    # Print summary
    success_count = sum(1 for r in results if r['status'] == 'Success')
    upload_success_count = sum(1 for r in results if r.get('gcs_upload') == 'Success')
    
    print(f"\nProcessing complete:")
    print(f"- {success_count}/{len(urls)} URLs successfully processed")
    if bucket_name:
        print(f"- {upload_success_count}/{success_count} files successfully uploaded to GCS")

if __name__ == "__main__":
    main()