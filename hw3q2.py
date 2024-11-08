import pymysql
import pandas as pd

# AWS RDS MySQL connection details
host = #
port = 3306  # Default MySQL port
database = 'hw3q2'
username = #
password = #

# Establish connection to AWS RDS MySQL
conn = pymysql.connect(
    host=host,
    user=username,
    password=password,
    database=database,
    port=port
)


cursor = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS stock_prices (
    ticker_symbol VARCHAR(10),
    day_date DATE,
    close_value FLOAT,
    volume BIGINT,
    open_value FLOAT,
    high_value FLOAT,
    low_value FLOAT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Load the CSV data
df = pd.read_csv('CompanyValues.csv')

# INSERTING ROW BY ROW TOOK SO LONG TO RUN, THEREFORE THIS BLOCK OF CODE HAD TO BE ADJUSTED
# # Insert data row by row into the table
# for index, row in df.iterrows():
#     insert_query = """
#     INSERT INTO stock_prices (ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value)
#     VALUES (%s, %s, %s, %s, %s, %s, %s);
#     """
#     cursor.execute(insert_query, (row['ticker_symbol'], row['day_date'], row['close_value'], row['volume'], row['open_value'], row['high_value'], row['low_value']))
#     conn.commit()

# Learned the concept of bulk insertion from chatGPT
# Prepare data for bulk insertion
data = [(row['ticker_symbol'], row['day_date'], row['close_value'], row['volume'], row['open_value'], row['high_value'], row['low_value'])
        for index, row in df.iterrows()]

# Bulk insert the data
insert_query = """
INSERT INTO stock_prices (ticker_symbol, day_date, close_value, volume, open_value, high_value, low_value)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
cursor.executemany(insert_query, data)
conn.commit()

# Querying the data to find the months with the highest trade volume
query = """
SELECT ticker_symbol, 
       DATE_FORMAT(day_date, '%Y-%m') AS month, 
       SUM(volume) AS total_volume
FROM stock_prices
# Adding AAPL and MSFT will only return those two since they have the top 100 highest volumes
WHERE ticker_symbol IN ('AMZN', 'GOOG', 'GOOGL', 'TSLA')
GROUP BY ticker_symbol, month
ORDER BY total_volume DESC
LIMIT 50;
"""

# Execute the query
cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# Convert the results into a DataFrame for easier handling
df_results = pd.DataFrame(results, columns=['ticker_symbol', 'month', 'total_volume'])

# Display the results
print(df_results)
