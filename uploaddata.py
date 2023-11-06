import pandas as pd
from sqlalchemy import create_engine
import creds

# The provided URL for the PostgreSQL database access
database_url = creds.url

# Set up a connection to the PostgreSQL database
engine = create_engine(database_url)

# Read the CSV file into a DataFrame
csv_file_path = 'combined_datasets.csv'
df = pd.read_csv(csv_file_path)

# Name of the table you want to insert data into
table_name = 'Transformed_WebAnalytics'

# Insert the DataFrame into the PostgreSQL table
# Change 'replace' to 'append' if you're adding to an existing table.
df.to_sql(table_name, engine, if_exists='replace', index=False)

print("Data inserted successfully.")