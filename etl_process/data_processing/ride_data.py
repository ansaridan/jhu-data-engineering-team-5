### File used to download 2023 (June - Decemeber) and 2024 (January - June) citibike monthly data ###
### Transforms columns to station id, month, hour, year, num_outbound, num_inbound and saves as iindividual csv by month ###

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


# url and file to download 2023
url = "https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip"
local_filename = "2023-citibike-tripdata.zip"

# Download the zipped file for 2023
with requests.get(url, stream=True) as response:
    response.raise_for_status()
    with open(local_filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

print(f"Downloaded {local_filename}")

# unizip main file - 2023
file = "2023-citibike-tripdata.zip"
with zipfile.ZipFile(file, "r") as main_zip:
    main_zip.extractall()

# unip month files of interest
file_list = [
    "202306-citibike-tripdata.zip",
    "202307-citibike-tripdata.zip",
    "202308-citibike-tripdata.zip",
    "202309-citibike-tripdata.zip",
    "202310-citibike-tripdata.zip",
    "202311-citibike-tripdata.zip",
    "202312-citibike-tripdata.zip",
]

main_path = "2023-citibike-tripdata/"
for file in file_list:
    total_path = main_path + file
    new_location = main_path + file[0:6]
    with zipfile.ZipFile(total_path, "r") as main_zip:
        main_zip.extractall(new_location)

# delete zip files
total_file_list = [
    "202301-citibike-tripdata.zip",
    "202302-citibike-tripdata.zip",
    "202303-citibike-tripdata.zip",
    "202304-citibike-tripdata.zip",
    "202305-citibike-tripdata.zip",
    "202306-citibike-tripdata.zip",
    "202307-citibike-tripdata.zip",
    "202308-citibike-tripdata.zip",
    "202309-citibike-tripdata.zip",
    "202310-citibike-tripdata.zip",
    "202311-citibike-tripdata.zip",
    "202312-citibike-tripdata.zip",
]

# Remove the main zip file - 2023
os.remove("2023-citibike-tripdata.zip")

# Remove each file in the file list
for file in total_file_list:
    path = f"2023-citibike-tripdata/{file}"
    if os.path.isfile(path):
        os.remove(path)
        print(f"Deleted file: {path}")
    else:
        print(f"File not found: {path}")

# function to delete local files
def delete_folder_contents(folder_path):
    if os.path.exists(folder_path):
        # Loop over all the files and directories in the folder
        for root, dirs, files in os.walk(folder_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                try:
                    os.remove(file_path)  # Delete file
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")
            for name in dirs:
                dir_path = os.path.join(root, name)
                try:
                    os.rmdir(dir_path)  # Delete directory
                except Exception as e:
                    print(f"Error deleting directory {dir_path}: {e}")
        # Attempt to delete the main folder
        try:
            os.rmdir(folder_path)
            print(f"Deleted {folder_path}")
        except Exception as e:
            print(f"Error deleting main folder {folder_path}: {e}")
    else:
        print(f"The directory {folder_path} does not exist.")


# function to determine if float
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


# function to clean data
def cleaning_data(df):

    # Convert 'started_at' and 'ended_at' columns to datetime
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])

    # Extract year, month, day, and hour from 'started_at'
    df["year_start"] = df["started_at"].dt.year
    df["month_start"] = df["started_at"].dt.month
    df["day_start"] = df["started_at"].dt.day
    df["hour_start"] = df["started_at"].dt.hour

    # Extract year, month, day, and hour from 'ended_at'
    df["year_end"] = df["ended_at"].dt.year
    df["month_end"] = df["ended_at"].dt.month
    df["day_end"] = df["ended_at"].dt.day
    df["hour_end"] = df["ended_at"].dt.hour

    # Group by 'start_station_id', 'year_start', 'month_start', 'day_start', and 'hour_start' and get count of outbound rides
    grouped_df_outbound = (
        df.groupby(
            ["start_station_id", "year_start", "month_start", "day_start", "hour_start"]
        )
        .size()
        .reset_index(name="num_outbound")
    )

    # Group by 'end_station_id', 'year_end', 'month_end', 'day_end', and 'hour_end' and get count of inbound rides
    grouped_df_inbound = (
        df.groupby(["end_station_id", "year_end", "month_end", "day_end", "hour_end"])
        .size()
        .reset_index(name="num_inbound")
    )

    # Rename columns to facilitate merging
    grouped_df_outbound = grouped_df_outbound.rename(
        columns={
            "start_station_id": "station_id",
            "year_start": "year",
            "month_start": "month",
            "day_start": "day",
            "hour_start": "hour",
        }
    )

    grouped_df_inbound = grouped_df_inbound.rename(
        columns={
            "end_station_id": "station_id",
            "year_end": "year",
            "month_end": "month",
            "day_end": "day",
            "hour_end": "hour",
        }
    )

    # Merge the two DataFrames on the common columns
    merged_df = pd.merge(
        grouped_df_outbound,
        grouped_df_inbound,
        on=["station_id", "year", "month", "day", "hour"],
        how="outer",
    )

    # Fill NaN values with 0
    merged_df["num_outbound"] = merged_df["num_outbound"].fillna(0)
    merged_df["num_inbound"] = merged_df["num_inbound"].fillna(0)

    return merged_df


# List all folder months we care about
folders = [f"2023{month:02d}" for month in range(6, 13)]

# count
count = 0

# define new folder path for cleaned csvs
new_folder_path = "cleaned_historical_data"

# Create the new folder if it doesn't exist
os.makedirs(new_folder_path, exist_ok=True)

for folder in folders:
    # empty list of dataframes
    dataframes = []
    folder_path = f"2023-citibike-tripdata/{folder}"
    contents = os.listdir(folder_path)
    for file in contents:
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            dtype = {"start_station_id": "object", "end_station_id": "object"}
            df = pd.read_csv(file_path, dtype=dtype)

            # drops all occurrences where station_id is not float
            df = df[
                df["start_station_id"].apply(is_float)
                & df["end_station_id"].apply(is_float)
            ]
            df["start_station_id"] = df["start_station_id"].astype(float)
            df["end_station_id"] = df["end_station_id"].astype(float)

            # adds all dataframes from same month to list
            dataframes.append(df)

    # Create one dataframe for the month
    combined_df = pd.concat(dataframes, ignore_index=True)

    cleaned_df = cleaning_data(combined_df)

    # write to new cleaned_csv
    cleaned_df.to_csv(
        f"cleaned_historical_data/cleaned_data_historical_{count}.csv", index=False
    )

    # Print progress
    print(f"Created {count}/12 csvs.")

    # increase count by 1
    count = count + 1

    # Force deletion to avoid quit
    del combined_df
    del cleaned_df
    gc.collect()


# Delete local files for 2023 citibike monthly data

# Path/folderto Delete
folder = "2023-citibike-tripdata"

# Use the function to delete the folder and its contents
delete_folder_contents(folder)

# Download and unzip 2024 data

local_filenames = [
    "202401-citibike-tripdata.csv.zip",
    "202402-citibike-tripdata.csv.zip",
    "202403-citibike-tripdata.csv.zip",
    "202404-citibike-tripdata.csv.zip",
    "202405-citibike-tripdata.zip",
    "202406-citibike-tripdata.zip",
]

for local_filename in local_filenames:

    url = f"https://s3.amazonaws.com/tripdata/{local_filename}"

    # Download the zipped file
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    # Unzip file
    with zipfile.ZipFile(local_filename, "r") as zip_ref:
        zip_ref.extractall(f"{local_filename[:-4]}")

    print(f"Downloaded and unzipped {local_filename[:-4]}")


# List all folders for 2024
folders = [filename[:-4] for filename in local_filenames]

for folder in folders:
    # empty list of dataframes
    dataframes = []
    contents = os.listdir(folder)
    for file in contents:
        if file.endswith(".csv"):
            file_path = os.path.join(folder, file)
            dtype = {"start_station_id": "object", "end_station_id": "object"}
            df = pd.read_csv(file_path, dtype=dtype)

            # drops all occurrences where station_id is not float
            df = df[
                df["start_station_id"].apply(is_float)
                & df["end_station_id"].apply(is_float)
            ]
            df["start_station_id"] = df["start_station_id"].astype(float)
            df["end_station_id"] = df["end_station_id"].astype(float)

            # adds all dataframes from same month to list
            dataframes.append(df)

    # Create one dataframe for the month
    combined_df = pd.concat(dataframes, ignore_index=True)

    cleaned_df = cleaning_data(combined_df)

    # write to new cleaned_csv
    cleaned_df.to_csv(
        f"cleaned_historical_data/cleaned_data_historical_{count}.csv", index=False
    )
    # Print progress
    print(f"Created {count}/12 csvs.")

    # increase count by 1
    count = count + 1

    # Force deletion to avoid kernel quit
    del combined_df
    del cleaned_df
    gc.collect()

# Delete local files 2024
for folder in folders:
    delete_folder_contents(folder)

for file in local_filenames:
    os.remove(file)
