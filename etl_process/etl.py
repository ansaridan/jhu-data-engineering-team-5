# ENSURE THIS RUNS CORRECTLY REGARDLESS OF DIRECTORY
import os
import sys
# Set the working directory to the project root
WORKDIR = "/home/awesome"
os.chdir(WORKDIR)
print(f"Current working directory: {os.getcwd()}")
# Add the project root to the Python path
sys.path.insert(0, WORKDIR)

# IMPORTS
# datatypes
import json
import yaml
# database connection
import psycopg2
import psycopg2.extras
import psycopg2.extensions as psql_ext
from psycopg2 import sql
# custom etl functions
from etl_process import utils as etl
# respective datasets
from etl_process.data_processing import station_info as info
# computation
import pandas as pd
# utilities
from pathlib import Path
import itertools
# typing
from typing import Union

# set up directories
WORKDIR_PATH = Path.cwd()
DATA_PATH = WORKDIR_PATH / 'etl_process' / 'processed_data'
SCHEMAS_PATH = WORKDIR_PATH / 'etl_process' / 'schemas'

PROJECT_SCHEMA = 'citibike_project'

def main():
    # PSQL db connection using psycopg2
    with psycopg2.connect(
        dbname='new_db', 
        user='awesome_user', 
        password='awesome_password', 
        host='postgres', 
        port='5432'
    ) as conn:
    
        # [1] SET UP THE DATABASE SCHEMA
        etl.drop_recreate_schema(conn, PROJECT_SCHEMA)
    
        # [2] SET UP THE TABLE SCHEMAS
        schema_files = [item for item in SCHEMAS_PATH.iterdir() if item.is_file()]
        tables_schemas = list(
            itertools.chain(*[etl.read_yaml_to_dict(schema_file)["tables"] for schema_file in schema_files])
        )
        tables_schemas = {k: v for d in tables_schemas for k, v in d.items()}
        
        for table_name, table_schema in tables_schemas.items():
            etl.drop_recreate_table(
                db_schema=PROJECT_SCHEMA,
                table_name=table_name,
                table_schema=table_schema,
                conn=conn,
            )
    
        # [3] UPLOAD EACH DATASET FROM CSVS
        #        3a: Ride Data
        traffic_data = [item for item in (DATA_PATH / "rides" / "station_traffic").iterdir() if item.is_file()]
        
        for file in traffic_data:
            df = pd.read_csv(file)
            etl.upload_dataframe(
                conn=conn,
                dataframe=df,
                db_schema=PROJECT_SCHEMA,
                table_name="citibike_station_history",
                table_schema=tables_schemas["citibike_station_history"]
            )
    
        #         3b: Weather Data
        for file in [
            "weather_general",
            "weather_precip",
            "weather_pressure",
            "weather_sky_coverage",
            "weather_wind",
            "weather_wxcode",
        ]:
            df = pd.read_csv(DATA_PATH / "weather" / f"{file}.csv")
            etl.upload_dataframe(
                conn=conn,
                dataframe=df,
                db_schema=PROJECT_SCHEMA,
                table_name=file,
                table_schema=tables_schemas[file]
            )
    
    
        #         3c: IRS Data
        for file in [
            "irs_codes",
            "nyc_irs",
        ]:
            df = pd.read_csv(DATA_PATH / "irs" / f"{file}.csv")
            etl.upload_dataframe(
                conn=conn,
                dataframe=df,
                db_schema=PROJECT_SCHEMA,
                table_name=file,
                table_schema=tables_schemas[file]
            )
            
        #         3d: Station Info
        df_station_info = info.get_station_info_data()
        
        etl.upload_dataframe(
            conn=conn,
            dataframe=df_station_info,
            db_schema=PROJECT_SCHEMA,
            table_name='station_info',
            table_schema=tables_schemas["station_info"]
        )

if __name__ == "__main__":
    main()
