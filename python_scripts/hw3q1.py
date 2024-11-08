from google.cloud import bigquery
from google.oauth2 import service_account
import matplotlib.pyplot as plt

#######################################################################
##
## Instructions to install required Python packages
##   for py
##   py -m pip install google-cloud
##   py -m pip install google-cloud-vision
##   py -m pip install google-cloud-bigquery

##  for python
##  python -m pip install google-cloud
##  python -m pip install google-cloud-vision
##  python -m pip install google-cloud-bigquery


##  for python3
##  python3 -m pip install google-cloud
##  python3 -m pip install google-cloud-vision
##  python3 -m pip install google-cloud-bigquery
##
########################################################################
## construct credentials from service account key file
credentials = service_account.Credentials.from_service_account_file('C:\\Users\\efeco\\OneDrive\\Masaüstü\\bigdata\\.bq_sa.json'
) ## relative file path

## construct a BigQuery client object
client = bigquery.Client(credentials=credentials)

## Your logics implementation goes below
# Perform a query.
# Correct query to fetch mobility data for the state of Georgia in March and April 2020
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

# Execute the query and convert the results to a Pandas DataFrame
df = client.query(QUERY).to_dataframe()

# Display the first few rows of the DataFrame
print(df.head())

# Check percentage of missing values in each column
print(df.isnull().mean() * 100)

# Handle the missing values
df['workplaces'].fillna(method='ffill', inplace=True)
df['retail_recreation'].fillna(method='ffill', inplace=True)
df['grocery_pharmacy'].fillna(method='ffill', inplace=True)

# Replace it with 0 instead because above 50% of the values are missing
df['transit_stations'].fillna(0, inplace=True)
df['residential'].fillna(0, inplace=True)


""" # Visualize the trends over time for each category
plt.figure(figsize=(10, 6))
plt.plot(df['date'], df['retail_recreation'], label="Retail and Recreation")
plt.plot(df['date'], df['grocery_pharmacy'], label="Grocery and Pharmacy")
plt.plot(df['date'], df['workplaces'], label="Workplaces")
plt.plot(df['date'], df['transit_stations'], label="Transit Stations")
plt.plot(df['date'], df['residential'], label="Residential")

# Add labels and title
plt.xlabel('Date')
plt.ylabel('Percent Change from Baseline')
plt.title('Movement Trends in Georgia (March-April 2020)')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()

# Show the plot
plt.show() """

# Visualize in subplots for better readability
fig, axs = plt.subplots(3, 2, figsize=(12, 10))

categories = ['retail_recreation', 'grocery_pharmacy', 'workplaces', 'transit_stations', 'residential']
titles = ['Retail & Recreation', 'Grocery & Pharmacy', 'Workplaces', 'Transit Stations', 'Residential']

# Code from GPT
for i, ax in enumerate(axs.flatten()[:-1]):  # last subplot will be empty
    ax.plot(df['date'], df[categories[i]], label=titles[i])
    ax.set_title(titles[i])
    ax.set_xlabel('Date')
    ax.set_ylabel('Percent Change')
    ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()
