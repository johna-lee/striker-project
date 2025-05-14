Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

Hello and welcome to my first data engineering project! This project is done from a hyptothetical perspective where I am a consultant for my favorite soccer club, Arsenal. Arsenal are in desperate need of a goalscorer to help them win titles, but they have budgetary constraints. My job is to identify the ideal value candidate; a player who is young, highly efficient, and up-and-coming, who does not command a big wage or transfer fee from the selling club.

To do this, I built the pipeline below which handles batch match data and leverages various services in Google Cloud Platform (GCP).

- Python+Pandas scrapes data from the web, saves as CSV, and uploads to Google Cloud Storage (GCS)
- GCS stores a copy of the raw data before processing to maintain data integrity
- Dataflow processes the hundreds of CSV files and loads the data in to BigQuery
- BigQuery serves as the data warehouse where transformations are made and the dataset is exported to PowerBI
- PowerBI allows for data analysis and data visualizations to present findings

The Data
The data I selected is for the past three seasons of two European soccer tournaments, UEFA Champions League (UCL) and UEFA Europa League (UEL), for a total of six initial tables comprising an aggregated dataset. The reason I chose these competitions is because although each country in European has a professional soccer league, the quality and competitiveness varies drastically. Someone who scores 30 goals in the Dutch Eredivise is doing so against inferior opposition compared to the English Premier League, so it is difficult to draw conclusions. However in the UCL and UEL the top teams from each country play each other, so performance should theoretically be a bit more predicitive. All match data was scraped from FBref.com, the most comprehensive free soccer database on the web.

Python+Pandas
To scrape the data, two separate scripts were needed. The first script (scrape_urls.py) was used to scrape the URLs for each match in each competition. As seen on the webpage for 2022-2023 UCL (https://fbref.com/en/comps/8/2022-2023/schedule/2022-2023-Champions-League-Scores-and-Fixtures), each match and result for the competition is listed in chronological order with the stats found by clicking the "Match Report" link. This is the link the script targets for the URL. The script works by looping through each row in the schedule table, finding the Match Report URL, saving it to an array, printing the array to a Pandas Dataframe, and saving the Dataframe as a CSV file. The resulting URLs for each match of the six competitions can be found in the folder named "urls", with the current year 2024-2025 competitions being as of May 3rd. I did utilize Claude.ai to refine the script by adding a request delay and headers, as the script was initially rate limited and blocked.

The second script (fbref_scraper.py) was used to iterate through the CSVs containing match URLs and extract the data for each player. As seen on the webpage for the first 2022-2023 UCL match (https://fbref.com/en/matches/07f058d4/Dinamo-Zagreb-Chelsea-September-6-2022-Champions-League), the main summary data is included in two tables, one for each team. However these tables are both sortable and contain multiple tabs of stats, which proved very challenging in defining the data I was looking for amongst the complex html. With some refinements in Claude.ai, I was able to identify the relevant tables based on key column headers. The script works by iterating through the CSV of URLs, extracting the match ID from the URL, finding the relevant tables, extracting the player data from both teams into a single Dataframe, inserting additional match ID and team name columns, saving the Dataframe as a CSV locally, uploading the local CSV to a Google Cloud Storage (GCS) bucket, and creating a processing report detailing the result of each upload (found in the folder named "GCS processing reports"). There is also some minor data cleaning and request delays/headers to prevent rate limiting.

GCS
