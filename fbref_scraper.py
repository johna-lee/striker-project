import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
from urllib.parse import urlparse
from google.cloud import storage

def create_folder_from_url(url):
    # Parse the URL
    parsed_url = urlparse(url)
    # Get the path part
    path = parsed_url.path
    
    # Extract match identifier from the URL
    match_id = re.search(r'/matches/([^/]+)/', path)
    
    if match_id:
        # Use match ID as part of folder name
        folder_name = f"fbref_match_{match_id.group(1)}"
    else:
        # Fallback: Use the last part of the path
        path_parts = path.strip('/').split('/')
        folder_name = f"fbref_{'_'.join(path_parts[-2:])}"
    
    # Replace any invalid characters for folder names
    folder_name = re.sub(r'[\\/*?:"<>|]', "_", folder_name)
    
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Created folder: {folder_name}")
    else:
        print(f"Folder already exists: {folder_name}")
    
    return folder_name

def extract_match_id(url):
    """
    Extract the match ID from the URL
    """
    match = re.search(r'/matches/([^/]+)/', url)
    if match:
        return match.group(1)
    return None

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

def save_tables_to_csv(all_tables, folder_name, match_id):
    saved_files = []
    for table_id, table_data in all_tables.items():
        # If there are multiple header rows, use the last one as column names
        header = table_data['header_rows'][-1] if table_data['header_rows'] else []
        
        # Create DataFrame
        df = pd.DataFrame(table_data['body_rows'], columns=header if header else None)
        
        # Add match_id column to each row
        df.insert(0, 'match_id', match_id)
        print(f"Added match_id column '{match_id}' to table {table_id}")
        
        # Process the Nation column if it exists
        if 'Nation' in df.columns:
            df['Nation'] = df['Nation'].apply(lambda x: x.split(' ', 1)[1] if ' ' in x else x)
            print(f"Processed 'Nation' column in table {table_id} - extracted text after space")
        
        # Save to CSV in the specified folder
        filename = os.path.join(folder_name, f"{table_id}.csv")
        df.to_csv(filename, index=False)
        print(f"Saved table {table_id} to {filename}")
        saved_files.append(filename)
    
    return saved_files

def delete_non_summary_files(folder_name):
    """
    Delete all files in the given folder that don't contain 'summary' in their filename.
    
    Args:
        folder_name (str): Path to the folder containing files to check
    """
    deleted_count = 0
    kept_count = 0
    kept_files = []
    
    # List all files in the folder
    for filename in os.listdir(folder_name):
        file_path = os.path.join(folder_name, filename)
        
        # Skip directories
        if os.path.isdir(file_path):
            continue
        
        # Check if file contains 'summary' in its name
        if 'summary' not in filename.lower():
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
        else:
            print(f"Kept: {file_path}")
            kept_count += 1
            kept_files.append(file_path)
    
    print(f"\nCleanup summary: Deleted {deleted_count} files, kept {kept_count} summary files.")
    return kept_files

def upload_to_gcs(local_file_paths, bucket_name, gcs_folder=None, project_id=None):
    """
    Upload files to Google Cloud Storage
    
    Args:
        local_file_paths (list): List of local file paths to upload
        bucket_name (str): Name of the GCS bucket
        gcs_folder (str, optional): Folder within the bucket to upload files to
        project_id (str, optional): Google Cloud project ID
    
    Returns:
        list: List of GCS URIs for the uploaded files
    """
    # Initialize GCS client
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        uploaded_uris = []
        
        for local_path in local_file_paths:
            # Get just the filename from the path
            filename = os.path.basename(local_path)
            
            # Create GCS blob path (with optional folder)
            if gcs_folder:
                blob_name = f"{gcs_folder}/{filename}"
            else:
                blob_name = filename
                
            # Create blob object
            blob = bucket.blob(blob_name)
            
            # Upload file
            blob.upload_from_filename(local_path)
            
            # Generate URI
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            uploaded_uris.append(gcs_uri)
            
            print(f"Uploaded {local_path} to {gcs_uri}")
            
        return uploaded_uris
        
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return []

def process_single_url(url, bucket_name, project_id):
    """Process a single FBref URL"""
    print(f"\n{'='*80}\nProcessing URL: {url}\n{'='*80}")
    
    # Extract match ID from URL
    match_id = extract_match_id(url)
    if match_id:
        print(f"Extracted match ID: {match_id}")
    else:
        print("Could not extract match ID from URL, using 'unknown' instead")
        match_id = "unknown"
    
    # Create folder based on URL
    folder_name = create_folder_from_url(url)
    
    # Scrape tables
    print("Scraping tables from FBref...")
    all_tables = scrape_fbref_tables(url)
    
    uploaded_uris = []
    if all_tables:
        print(f"Found {len(all_tables)} tables with thead and tbody elements.")
        
        # Print the table IDs
        print("Table IDs:")
        for table_id in all_tables.keys():
            print(f" - {table_id}")
        
        # Save tables to CSV in the created folder with match ID column
        saved_files = save_tables_to_csv(all_tables, folder_name, match_id)
        
        # Delete files that don't contain "summary" in their names
        print("\nCleaning up files - keeping only 'summary' files...")
        kept_files = delete_non_summary_files(folder_name)
        
        # Upload remaining files to Google Cloud Storage
        if kept_files:
            print("\nUploading files to Google Cloud Storage...")
            gcs_folder = f"fbref_data/{match_id}"  # Organize files by match ID
            uploaded_uris = upload_to_gcs(kept_files, bucket_name, gcs_folder, project_id)
            
            if uploaded_uris:
                print("\nSuccessfully uploaded files to GCS:")
                for uri in uploaded_uris:
                    print(f" - {uri}")
            else:
                print("\nNo files were uploaded to GCS.")
        else:
            print("\nNo summary files to upload.")
    else:
        print("No tables found or scraping failed.")
    
    return match_id, uploaded_uris

def main():
    # CSV file containing the URLs
    urls_csv_path = "fbref_urls.csv"  # Replace with your CSV file path
    
    # Google Cloud Storage configuration
    bucket_name = "striker-project"  # Replace with your bucket name
    project_id = "striker-project-457523"    # Replace with your project ID
    
    # Check if CSV file exists
    if not os.path.exists(urls_csv_path):
        print(f"Error: CSV file '{urls_csv_path}' not found.")
        return
    
    # Read URLs from CSV file
    try:
        # Attempt to read URLs column
        urls_df = pd.read_csv(urls_csv_path)
        
        # Determine the column name containing URLs
        url_column = None
        potential_columns = ['url', 'URL', 'link', 'LINK', 'fbref_url', 'match_url']
        
        for col in potential_columns:
            if col in urls_df.columns:
                url_column = col
                break
        
        # If none of the expected columns were found, use the first column
        if url_column is None:
            url_column = urls_df.columns[0]
            print(f"No standard URL column name found. Using first column: '{url_column}'")
        
        # Extract URLs
        urls = urls_df[url_column].tolist()
        
        if not urls:
            print("No URLs found in the CSV file.")
            return
        
        print(f"Found {len(urls)} URLs in '{urls_csv_path}'")
        
        # Process each URL
        results = []
        for i, url in enumerate(urls):
            print(f"\nProcessing URL {i+1}/{len(urls)}")
            try:
                match_id, uploaded_files = process_single_url(url, bucket_name, project_id)
                results.append({
                    'url': url,
                    'match_id': match_id,
                    'success': bool(uploaded_files),
                    'files_uploaded': len(uploaded_files)
                })
                
                # Add a delay between requests to avoid rate limiting
                if i < len(urls) - 1:  # Don't sleep after the last URL
                    print("Waiting 2 seconds before processing next URL...")
                    time.sleep(2)
            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                results.append({
                    'url': url,
                    'match_id': None,
                    'success': False,
                    'files_uploaded': 0,
                    'error': str(e)
                })
        
        # Create summary report
        summary_df = pd.DataFrame(results)
        summary_path = "fbref_scraping_results.csv"
        summary_df.to_csv(summary_path, index=False)
        print(f"\nScraping summary saved to {summary_path}")
        
        # Print overall statistics
        successful = summary_df['success'].sum()
        print(f"\nProcessing complete: {successful}/{len(urls)} URLs successfully processed")
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")

if __name__ == "__main__":
    main()