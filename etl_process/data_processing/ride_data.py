import requests
import zipfile
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import gc
import warnings 
import numpy as np

# Suppress warnings
warnings.filterwarnings("ignore")

# Drop tables if exist to avoid double adding

# Database connection parameters
dbname = 'new_db'
user = 'awesome_user'
password = 'awesome_password'
host = 'localhost'
port = '5432'

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
c = conn.cursor()

# Define the schema to be dropped if it exists and everything in it
query = "DROP SCHEMA IF EXISTS historical_data CASCADE;"
# Execute the drop table command
c.execute(query)
conn.commit()

# Close the cursor and connection
c.close()
conn.close()

print(f"Schema dropped successfully.")

url = 'https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip'
local_filename = '2023-citibike-tripdata.zip'

# Download the zipped file for 2023
with requests.get(url, stream=True) as response:
    response.raise_for_status()
    with open(local_filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=81921048576):
            file.write(chunk)

print(f'Downloaded {local_filename}')

# Unzip 2023 file 
with zipfile.ZipFile(local_filename, 'r') as zip_ref:
    zip_ref.extractall('citibike_data_2023')

print('Unzipped the file.')

# function to clean data
def cleaning_data(df): 
    
    # Convert 'started_at' and 'ended_at' columns to datetime
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    
    # Extract year, month, day, and hour from 'started_at'
    df['year_start'] = df['started_at'].dt.year
    df['month_start'] = df['started_at'].dt.month
    df['day_start'] = df['started_at'].dt.day
    df['hour_start'] = df['started_at'].dt.hour
    
    # Extract year, month, day, and hour from 'ended_at'
    df['year_end'] = df['ended_at'].dt.year
    df['month_end'] = df['ended_at'].dt.month
    df['day_end'] = df['ended_at'].dt.day
    df['hour_end'] = df['ended_at'].dt.hour
    
    # Group by 'start_station_id', 'year_start', 'month_start', 'day_start', and 'hour_start' and get count of outbound rides
    grouped_df_outbound = df.groupby(['start_station_id', 'year_start', 'month_start', 'day_start', 'hour_start']).size().reset_index(name='num_outbound')
    
    # Group by 'end_station_id', 'year_end', 'month_end', 'day_end', and 'hour_end' and get count of inbound rides
    grouped_df_inbound = df.groupby(['end_station_id', 'year_end', 'month_end', 'day_end', 'hour_end']).size().reset_index(name='num_inbound')
    
    # Rename columns to facilitate merging
    grouped_df_outbound = grouped_df_outbound.rename(columns={
        'start_station_id': 'station_id',
        'year_start': 'year',
        'month_start': 'month',
        'day_start': 'day',
        'hour_start': 'hour'
    })

    grouped_df_inbound = grouped_df_inbound.rename(columns={
        'end_station_id': 'station_id',
        'year_end': 'year',
        'month_end': 'month',
        'day_end': 'day',
        'hour_end': 'hour'
    })
    
    # Merge the two DataFrames on the common columns
    merged_df = pd.merge(grouped_df_outbound, grouped_df_inbound, on=['station_id', 'year', 'month', 'day', 'hour'], how='outer')
    
    # Fill NaN values with 0 
    merged_df['num_outbound'] = merged_df['num_outbound'].fillna(0)
    merged_df['num_inbound'] = merged_df['num_inbound'].fillna(0)

    return(merged_df)

# function to extract station information
def station_extraction(df): 
    # Extract unique start station information
    start_stations = df[['start_station_id', 'start_station_name', 'start_lat', 'start_lng']].drop_duplicates()
    start_stations = start_stations.rename(columns={
        'start_station_id': 'station_id',
        'start_station_name': 'station_name',
        'start_lat': 'lat',
        'start_lng': 'lng'
    })
    
    # Extract unique end station information
    end_stations = df[['end_station_id', 'end_station_name', 'end_lat', 'end_lng']].drop_duplicates()
    end_stations = end_stations.rename(columns={
        'end_station_id': 'station_id',
        'end_station_name': 'station_name',
        'end_lat': 'lat',
        'end_lng': 'lng'
    })
    
    # Combine start and end station information
    all_stations = pd.concat([start_stations, end_stations]).drop_duplicates().reset_index(drop=True)
    
    return(all_stations)

def insert_database(df, table_name, db_credentials):

    df = df 
    # Establish database connection
    conn = psycopg2.connect(dbname=db_credentials["dbname"], 
                            user=db_credentials["user"], 
                            password=db_credentials["password"], 
                            host=db_credentials["host"],
                            port=db_credentials["port"])
        
    # Creating a cursor object using the cursor() method
    c = conn.cursor()

    # Define the new schema name
    new_schema = 'historical_data'

    # Create the new schema
    cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(new_schema)))
        
    if table_name == "citibike_station_history":
        c.execute('''CREATE TABLE IF NOT EXISTS historical_data.citibike_station_history(
              station_id REAL, 
              year INT,
              month INT, 
              day INT, 
              hour INT, 
              num_inbound INT,
              num_outbound INT, 
              PRIMARY KEY (station_id, year, month, day, hour)
              )
              ''')
        conn.commit()

        insert_query = '''INSERT INTO historical_data.citibike_station_history (
        station_id, 
        year,
        month,
        day,
        hour,
        num_inbound, 
        num_outbound
                ) VALUES %s
        ON CONFLICT (station_id, year, month, day, hour)
        DO UPDATE SET
        num_inbound = citibike_station_history.num_inbound + EXCLUDED.num_inbound,
        num_outbound = citibike_station_history.num_outbound + EXCLUDED.num_outbound'''
    
    elif table_name == "station_info": 
        c.execute('''CREATE TABLE IF NOT EXISTS historical_data.station_info(
                  station_id INT PRIMARY KEY, 
                  name TEXT,
                  latitude REAL, 
                  longitude REAL
                  )
                  ''')
        conn.commit()

        insert_query = '''INSERT INTO historical_data.station_info (
        station_id, 
        name,
        latitude,
        longitude
        ) VALUES %s'''

    # Convert to list of tuples for batch insert and ensure native Python types
    flat_df = [tuple(map(lambda x: x.item() if isinstance(x, np.generic) else x, row)) for row in df.to_numpy()]
    
    # Execute batch insert
    execute_values(c, insert_query, flat_df)
    conn.commit()

    # Close cursor and connection
    c.close()
    conn.close()


# function to delete local files
def delete_folder_contents(folder_path):
    if os.path.exists(folder_path):
        # Loop over all the files and directories in the main folder
        for root, dirs, files in os.walk(folder_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                os.remove(file_path)
            for name in dirs:
                dir_path = os.path.join(root, name)
                os.rmdir(dir_path)
        # Delete main folder 
        os.rmdir(folder_path)
    else:
        print(f"The directory {folder_path} does not exist.")

# function to determine if float
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# empty list to store station info dataframes for each month
station_info_1 = []
# List all folder months we care about
folders = ["6_June", "7_July", "8_August", 
           "9_September", "10_October", "11_November", 
           "12_December"]

for folder in folders:
    # empty list of dataframes
    dataframes = []
    folder_path = f'citibike_data_2023/2023-citibike-tripdata/{folder}'
    contents = os.listdir(folder_path)
    for file in contents:
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            dtype = {"station_id":"object"}
            df = pd.read_csv(file_path, dtype=dtype)
            
            # drops all occurrences where station_id is not float
            df = df[df['start_station_id'].apply(is_float) & df['end_station_id'].apply(is_float)]
            df['start_station_id'] = df['start_station_id'].astype(float)
            df['end_station_id'] = df['end_station_id'].astype(float)

            # adds all dataframes from same month to list
            dataframes.append(df)

    # Create one dataframe for the month
    combined_df = pd.concat(dataframes, ignore_index=True)

    cleaned_df = cleaning_data(combined_df)

    # insert data into postgres sql database
    db_credentials = {"dbname": "new_db", 
                  "user":"awesome_user",
                  "password": "awesome_password",
                  "host": "localhost", 
                  "port": "5432"}
    
    insert_database(cleaned_df, "citibike_station_history", db_credentials)

    # add station information to list
    station_info_1.append(station_extraction(combined_df))

    #Force deletion to avoid quit
    del combined_df
    del cleaned_df
    gc.collect()

# Combine DataFrames
combined_station_1 = pd.concat(station_info_1)

# Drop duplicate rows
combined_station_1 = combined_station_1.drop_duplicates()

# Write to csv
combined_station_1.to_csv('station1.csv', index=False)

# Force deletion to avoid quit
del combined_station_1
del station_info_1
gc.collect()

# Delete local files for 2023 citibike monthly data

# Path/file to Delete
folder_path_1 = 'citibike_data_2023'
file = '2023-citibike-tripdata.zip'

# Use the function to delete the folder and its contents
delete_folder_contents(folder_path_1)
os.remove(file)

# Download and unzip 2024 data

local_filenames = ['202401-citibike-tripdata.csv.zip',
                   '202402-citibike-tripdata.csv.zip',
                   '202403-citibike-tripdata.csv.zip',
                   '202404-citibike-tripdata.zip',
                   '202405-citibike-tripdata.zip',
                   '202406-citibike-tripdata.zip']
                   
for local_filename in local_filenames:
                   
    url = f'https://s3.amazonaws.com/tripdata/{local_filename}'
    
    # Download the zipped file
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=81921048576):
                file.write(chunk)
    # Unzip file 
    with zipfile.ZipFile(local_filename, 'r') as zip_ref:
        zip_ref.extractall(f'{local_filename[:-4]}')
    
    print(f'Downloaded and unzipped {local_filename[:-4]}')

local_filenames = ['202401-citibike-tripdata.csv.zip',
                   '202402-citibike-tripdata.csv.zip',
                   '202403-citibike-tripdata.csv.zip',
                   '202404-citibike-tripdata.zip',
                   '202405-citibike-tripdata.zip',
                   '202406-citibike-tripdata.zip']

# empty list for 2024 station info
station_info_2 = []

# List all folders for 2024
folders = [filename[:-4] for filename in local_filenames]

for folder in folders:
    # empty list of dataframes
    dataframes = []
    contents = os.listdir(folder)
    for file in contents:
        if file.endswith('.csv'):
            file_path = os.path.join(folder, file)
            dtype = {"station_id":"object"}
            df = pd.read_csv(file_path, dtype=dtype)
            
            # drops all occurrences where station_id is not float
            df = df[df['start_station_id'].apply(is_float) & df['end_station_id'].apply(is_float)]
            df['start_station_id'] = df['start_station_id'].astype(float)
            df['end_station_id'] = df['end_station_id'].astype(float)

            # adds all dataframes from same month to list
            dataframes.append(df)

    # Create one dataframe for the month
    combined_df = pd.concat(dataframes, ignore_index=True)

    cleaned_df = cleaning_data(combined_df)

    # insert data into postgres sql database
    db_credentials = {"dbname": "new_db", 
                  "user":"awesome_user",
                  "password": "awesome_password",
                  "host": "localhost", 
                  "port": "5432"}
    
    insert_database(cleaned_df, "citibike_station_history", db_credentials)

    # add station information to list
    station_info_2.append(station_extraction(combined_df))

    # Force deletion to avoid kernel quit
    del combined_df
    del cleaned_df
    gc.collect()

# Delete local files 2024
for folder in folders: 
    delete_folder_contents(folder)

for file in local_filenames:
    os.remove(file)

# combine 2023 and 2024 station info drop duplicates
station_1 = pd.read_csv("station1.csv")
station_2 = pd.concat(station_info_2)
station_2 = station_2.drop_duplicates()

station_combined = pd.concat([station_1, station_2]).drop_duplicates()
station_combined = station_combined.drop_duplicates()

table_name = "station_info"

db_credentials = {"dbname": "new_db", 
                  "user":"awesome_user",
                  "password": "awesome_password",
                  "host": "localhost", 
                  "port": "5432"}
insert_database(station_combined, table_name , db_credentials)
