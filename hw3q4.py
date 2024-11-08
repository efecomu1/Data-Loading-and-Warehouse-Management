import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.oauth2 import service_account

# Connect to MySQL database (adjust credentials as needed)
db_connection = pymysql.connect(
    host='localhost',
    user=#,
    password=#,
    database='star_schema'
)
engine = create_engine('mysql+pymysql://root:EfeBjk1903@localhost/star_schema')

# PART 1: Google Cloud Mobility Data
credentials = service_account.Credentials.from_service_account_file('C:\\Users\\efeco\\OneDrive\\Masaüstü\\bigdata\\.bq_sa.json')
client = bigquery.Client(credentials=credentials)

# Mobility query
QUERY = """
SELECT date, retail_and_recreation_percent_change_from_baseline AS retail_recreation, 
       grocery_and_pharmacy_percent_change_from_baseline AS grocery_pharmacy, 
       workplaces_percent_change_from_baseline AS workplaces, 
       transit_stations_percent_change_from_baseline AS transit_stations, 
       residential_percent_change_from_baseline AS residential 
FROM `bigquery-public-data.covid19_google_mobility.mobility_report`
WHERE country_region = "United States"
  AND sub_region_1 = "Georgia"
  AND date BETWEEN '2020-03-01' AND '2020-04-30'
ORDER BY date;
"""
mobility_data = client.query(QUERY).to_dataframe()

# PART 2: Stock Prices Data
# Load stock prices data from CSV
stock_data = pd.read_csv('CompanyValues.csv')

# PART 3: Social Mentions Data (Tweets)
tweets_query = """
SELECT
    FROM_UNIXTIME(Date, '%%Y-%%m-%%d') AS Date,
    CASE
        WHEN Text REGEXP 'AAPL' THEN 'Apple'
        WHEN Text REGEXP 'TSLA' THEN 'Tesla'
        WHEN Text REGEXP 'MSFT' THEN 'Microsoft'
        WHEN Text REGEXP 'GOOGL' THEN 'Google'
        ELSE 'Other'
    END AS Company,
    COUNT(*) AS Mentions
FROM tweets
WHERE FROM_UNIXTIME(Date, '%%Y-%%m') BETWEEN '2020-03' AND '2020-04'
GROUP BY Company, Date;
"""
engine2 = create_engine('mysql+pymysql://root:EfeBjk1903@localhost/hw3q3')
tweets_data = pd.read_sql(tweets_query, engine2)

# Ensure 'Date' column is formatted as datetime in both DataFrames
stock_data['day_date'] = pd.to_datetime(stock_data['day_date'])
tweets_data['Date'] = pd.to_datetime(tweets_data['Date'])
stock_data.rename(columns={'day_date': 'Date'}, inplace=True)

# Convert the 'date' column in mobility_data to datetime
mobility_data['date'] = pd.to_datetime(mobility_data['date'])

# Merge stock_data and tweets_data on 'Date'
merged_data = pd.merge(stock_data, tweets_data, on='Date')

# Ensure date columns in both DataFrames are aligned and merge on 'Date'
merged_data = pd.merge(merged_data, mobility_data[['date', 'retail_recreation']], left_on='Date', right_on='date', how='left')

merged_data.rename(columns={'retail_recreation': 'MobilityTrend'}, inplace=True)

# Prepare the unique dates for insertion
merged_data['Date'] = pd.to_datetime(merged_data['Date'])
merged_data['Day'] = merged_data['Date'].dt.day
merged_data['Month'] = merged_data['Date'].dt.month
merged_data['Year'] = merged_data['Date'].dt.year
merged_data['DayOfWeek'] = merged_data['Date'].dt.day_name()

unique_dates = merged_data[['Date', 'Day', 'Month', 'Year', 'DayOfWeek']].drop_duplicates()

# SQL for inserting data with ON DUPLICATE KEY UPDATE
insert_query = """
INSERT INTO dim_date (`Date`, `Day`, `Month`, `Year`, `DayOfWeek`)
VALUES (:Date, :Day, :Month, :Year, :DayOfWeek)
ON DUPLICATE KEY UPDATE
    `Day`=VALUES(`Day`), 
    `Month`=VALUES(`Month`), 
    `Year`=VALUES(`Year`), 
    `DayOfWeek`=VALUES(`DayOfWeek`);
"""

# Inserting rows with raw SQL
with engine.begin() as conn:
    for _, row in unique_dates.iterrows():
        # Convert row to a dictionary of column names and values
        conn.execute(text(insert_query), {
            'Date': row['Date'], 
            'Day': row['Day'], 
            'Month': row['Month'], 
            'Year': row['Year'], 
            'DayOfWeek': row['DayOfWeek']
        })

print("Dates inserted or updated successfully!")


# SQL query for inserting data with ON DUPLICATE KEY UPDATE
mobility_insert_query = """
INSERT INTO dim_mobility (MobilityType, Description)
VALUES (:MobilityType, :Description)
ON DUPLICATE KEY UPDATE
    Description = VALUES(Description);
"""

# Mobility types data
mobility_types = pd.DataFrame({
    'MobilityType': ['retail', 'transit', 'residential', 'workplaces'],
    'Description': ['Retail and Recreation Mobility', 'Transit Station Mobility', 'Residential Mobility', 'Workplaces Mobility']
})

# Insert or update rows in dim_mobility table
with engine.begin() as conn:
    for _, row in mobility_types.iterrows():
        conn.execute(text(mobility_insert_query), {
            'MobilityType': row['MobilityType'], 
            'Description': row['Description']
        })

print("Mobility types inserted or updated successfully!")


# Step to populate fact_performance table

# Map company names to their respective IDs in dim_company (assuming they are already populated)
company_map = {
    'Apple': 1,       
    'Tesla': 2,       
    'Microsoft': 3,   
    'Google': 4      
}

# Add the company IDs to the merged data
merged_data['CompanyID'] = merged_data['Company'].map(company_map)

# Check for any NaNs in the CompanyID column, and drop these rows (this handles cases where the company mapping failed)
merged_data = merged_data.dropna(subset=['CompanyID'])

# Prepare the fact data
fact_data = merged_data[['Date', 'CompanyID', 'close_value', 'Mentions', 'MobilityTrend']].copy()

# Rename 'close_value' to 'StockPerformance' to match the fact_performance schema
fact_data.rename(columns={'close_value': 'StockPerformance'}, inplace=True)

# Replace any NaNs in MobilityTrend, StockPerformance, and SocialMentions with None (SQL NULL)
fact_data['MobilityTrend'] = fact_data['MobilityTrend'].apply(lambda x: None if pd.isna(x) else x)
fact_data['StockPerformance'] = fact_data['StockPerformance'].apply(lambda x: None if pd.isna(x) else x)
fact_data['SocialMentions'] = fact_data['Mentions'].apply(lambda x: None if pd.isna(x) else x)

# Check for any NaNs in fact_data before inserting into the database
fact_data.dropna(inplace=True)

# SQL query for inserting or updating fact_performance data
fact_insert_query = """
INSERT INTO fact_performance (Date, CompanyID, StockPerformance, SocialMentions, MobilityTrend)
VALUES (:Date, :CompanyID, :StockPerformance, :SocialMentions, :MobilityTrend)
ON DUPLICATE KEY UPDATE
    StockPerformance = VALUES(StockPerformance),
    SocialMentions = VALUES(SocialMentions),
    MobilityTrend = VALUES(MobilityTrend);
"""

# Insert or update rows in fact_performance table
with engine.begin() as conn:
    for _, row in fact_data.iterrows():
        # Ensure NaNs are replaced with None
        conn.execute(text(fact_insert_query), {
            'Date': row['Date'],
            'CompanyID': row['CompanyID'],
            'StockPerformance': row['StockPerformance'],
            'SocialMentions': row['SocialMentions'],
            'MobilityTrend': row['MobilityTrend']
        })

print("Fact performance data inserted or updated successfully!")


db_connection.close()

