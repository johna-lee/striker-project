Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

Hello and welcome to my first data engineering project! This project is done from a hyptothetical perspective, where I am a consultant for my favorite soccer club Arsenal. Arsenal are in desperate need of a goalscorer to help them win titles, but they have budgetary constraints. My job is to identify the ideal value candidate; a player who is young, highly efficient, up-and-coming, and does not command a big wage or transfer fee from the selling club.

To do this, I built the pipeline below which handles batch match data and leverages various services in Google Cloud Platform (GCP).

- Python+Pandas is used to scrape data from the web, save as CSV, and upload to Google Cloud Storage (GCS)
- GCS stores a copy of the raw data before processing to maintain data integrity
- Dataflow is responsible for processing the hundreds of CSV files and loading the data in to BigQuery
- BigQuery serves as the data warehouse where transformations are made and the dataset is exported to PowerBI
- PowerBI allows for data analysis and data visualizations to present our findings

