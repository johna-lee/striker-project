Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

Hello and welcome to my first data engineering project! This project is done from a hyptothetical perspective where I am a consultant for my favorite soccer club, Arsenal. Arsenal are in desperate need of a "striker", an attacking forward whose main job is to score goals, but they have budgetary constraints. My job is to identify the ideal value candidate; a player who is young, highly efficient, and up-and-coming, who does not command a big transfer fee from the selling club.

To do this, I built the pipeline below which handles batch match data and leverages various services in Google Cloud Platform (GCP).

- Python+Pandas scrapes data from the web, saves as CSV, and uploads to Google Cloud Storage (GCS)
- GCS stores a copy of the raw data before processing to maintain data integrity
- Dataflow processes the hundreds of CSV files and loads the data in to BigQuery
- BigQuery serves as the data warehouse where transformations are made and the dataset is exported to PowerBI
- PowerBI allows for data analysis and data visualizations to present findings

[comment]: <> (Insert pipeline diagram)

The Data
The data I selected is from the past three seasons of two European soccer tournaments, UEFA Champions League (UCL) and UEFA Europa League (UEL), for a total of six initial tables comprising an aggregated dataset. The reason I chose these competitions is because although each country in European has a professional soccer league, the quality and competitiveness varies drastically. Someone who scores 30 goals in the Dutch Eredivise is doing so against inferior opposition compared to the English Premier League, so it is difficult to draw conclusions across leagues. However in the UCL and UEL the top teams from each country play each other, so performance should theoretically be a bit more predicitive. All match data was scraped from FBref.com, the most comprehensive free soccer database on the web.

The Data Pipeline
Python+Pandas
To scrape the data, two separate scripts were needed. The first script (scrape_urls.py) was used to scrape the URLs for each match in each competition. As seen on the webpage for 2022-2023 UCL (https://fbref.com/en/comps/8/2022-2023/schedule/2022-2023-Champions-League-Scores-and-Fixtures), each match and result for the competition is listed in chronological order, with the stats found by clicking the "Match Report" link. This is the link the script targets for extracting the URL. The script works by looping through each row in the schedule table, finding the Match Report URL, saving it to an array, printing the array to a Pandas Dataframe, and saving the Dataframe as a CSV file. The resulting URLs of the six competitions can be found in the folder named "match report URLs", with the current year 2024-2025 competitions being as of May 3rd. I utilized Claude.ai to refine the script by adding a request delay and headers, as the script was initially rate limited and blocked.

The second script (fbref_scraper.py) was used to iterate through the match report URL CSVs and extract the data for each player. As seen on the webpage for the first 2022-2023 UCL match (https://fbref.com/en/matches/07f058d4/Dinamo-Zagreb-Chelsea-September-6-2022-Champions-League), the main summary data is included in two tables, one for each team. However these tables are sortable and contain multiple tabs of stats, which proved very challenging in defining the data I was looking for amongst the complex html. With some refinements in Claude.ai, I was able to identify the relevant tables based on key column headers. The script works by iterating through the URL CSVs, extracting the match ID from the URL, finding the relevant tables, extracting the player data from both teams into a single Dataframe, inserting additional match ID and team name columns, saving the Dataframe as a CSV locally, uploading the local CSV to a Google Cloud Storage (GCS) bucket, and creating a processing report detailing the result of each upload (found in the folder named "GCS processing reports"). There is also some minor data cleaning and request delays/headers to prevent rate limiting.

[comment]: <> (Insert python+pandas GCS upload screenshot)

GCS
After each competition's data was scraped and uploaded to the GCS bucket, I moved the CSV files to a named folder by yearly competition, which are the folders labeled "22-23 UCL match data", "22-23 UEL match data", etc. This serves as a record of the original files as well as allowing a single path for Dataflow to target when loading into BigQuery.

[comment]: <> (Insert GCS buckets screenshot)

Dataflow
With the data in Google Cloud Platform (GCP), the next step was to load the roughly 900 CSV files into a BigQuery data warehouse using Dataflow. However, two things needed to be done before that could happen. First, I created a schema file (bigquery_schema.json) which defines the column names and data types of the output tables.

Second, a total of seven output tables were created in Bigquery, one for each of the six competitions' successfully loaded data and one for errors or data that was not loaded. These tables can be seen in the diagram below, with the errors table on the bottom left titled "Insert bad records into Bigquery" and the competition table on the bottom right titled "Insert good records into Bigquery".

[comment]: <> (Insert Dataflow diagram screenshot)

A Dataflow job was created for the initial competition and cloned for the remaining ones thereafter. By targeting the competition folder in the GCS bucket, Dataflow read each file and loaded the data into the respective BigQuery tables. No errors occurred during the Dataflow jobs.

BigQuery
After the data was loaded into BigQuery tables, simple transformations were peformed. The first was creating two new columns, one titled "competition" and another "season", as these are not found in the scraped data. The screenshot below shows the SQL query used, which created the columns and set the values for each record. This query was cloned and adjusted for each respective table.

[comment]: <> (Insert Bigquery transformations screenshot)

The next action performed in BigQuery was to merge all six tables together into a single table. The screenshot below shows the SQL query used which unions all data from each table. The query results were saved as a new table "match_data" using the console interface.

[comment]: <> (Insert Bigquery merge screenshot)

Lastly, the age column had to be transformed from a string year-days format to an integer year format. The below shows the SQL queries used, where a temporary column was created, nulls assigned 0, string split on "-" delimiter, age column deleted, and the new updated column renamed to age.

[comment]: <> (Insert combined age query screenshots)

Power BI Desktop
With the dataset now complete, the final step of the data pipeline was to import the merged "match_data" table into Power BI Desktop for analysis, as shown below.

[comment]: <> (Insert PowerBI load table screenshot)

The Analysis
Although data has been extracted, loaded, transformed, and ready for analysis, my job of finding an attacker is just beginning! The following bullet points highlight each stage of my analysis and the thought process behind it. Since I am familiar with pivot table formats, I chose a matrix table for my initial visualization.

    - Player and Age - The first fields added to the matrix were "player" and a filter on age.

    - Filtering on Age - Age is one of the main criteria I have been tasked with in my search for the ideal player. While age is highly subjective with people peaking at different times, the general consensus is that attackers reach their prime in their mid-to-late 20s. As such, I set the Age summarization to Maximum and filtered for players less than 27 years old.

[comment]: <> (Insert ages less than 27 screenshot)

    - Filtering on "W" - As mentioned, I am looking for an attacker. Luckily, attackers in the forward line all contain a "W" in the position description; forwards are "FW", while left and right wingers are "LW" and "RW" respectively. To ensure all relevant data is captured, an advanced filter containing "W" is applied to position as shown below.

[comment]: <> (Insert PowerBI contains W screenshot)

    - Total Goals - I added a sum of goals and sorted descending to bring potential candidates into the visual field of the matrix table.

    - Goals per 90 - Goals per 90 is a metric used to determine the average number of goals per 90 minutes, or the length of a full soccer match. The reason it is important is because players can be substituted, and using just the number of games played is flawed. For example, if a player comes into two consecutive games at the 85th minute and scores a goal in each, it is technically not inaccurate to say they averaged one goal per game. However, scoring two goals having only been on the field a total 10 minutes across both games is much more impressive than someone who scored two goals playing two full games, or roughly 180 minutes.

    To add a Goals per 90 column, a new measure was created with the following calculation:
    goals_per_90 = ((SUM(match_data[goal]) / SUM(match_data[minute])) * 90)

    While Goals per 90 is a powerful metric, it cannot be relied on exclusively. As shown below, outliers can skew the data when minutes played are not also taken into consideration.

    - Goals per 90 by Minutes - By plotting Goals per 90 by total Minutes, an interesting visual appears. For the overwhelming majority of players, the more total Minutes played, the lower their Goals per 90 trends. There are however a small number of players who do not follow this trend circled below, and these are the players I want to focus on. These players are maintaining a consistently high Goals per 90 despite an increasing number of total minutes. New filters of >=1000 Minutes and >=0.5 Goals per 90 were added to the dataset.

Efficiency Analysis

    Goals to Expected Goals Ratio – Expected Goals (xG) is a metric that measures the quality of a goal-scoring opportunity by calculating the likelihood that it will be scored, based on data from similar shots in the past. In other words, a shot with an xG of 0.5 is expected to be scored half the time. Comparing the number of goals scored to xG provides a measure of efficiency, with a ratio greater than 1 indicating that a player is overperforming, while a ratio less than 1 indicates underperforming.

To add a Goals to Expected Goals Ratio column, a new measure was created with the following calculation:
goals_to_xg_ratio = SUM(match_data[goal]) / SUM(match_data[expected_goal])

Note: The Goals to Expected Goals Ratio is not adjusted to “per 90,” since calculating a "Goals per 90 to Expected Goals per 90" ratio yields the same result.

Shot Conversion Rate – Shot Conversion Rate is the percentage of shots that result in goals. This measures an attacker’s clinical finishing ability and is another indicator of efficiency in front of goal.

To add a Shot Conversion Rate column, a new measure was created with the following calculation:
shot_conversion_rate = (SUM(match_data[goal]) / SUM(match_data[shot])) * 100

Value Analysis



Conclusion


