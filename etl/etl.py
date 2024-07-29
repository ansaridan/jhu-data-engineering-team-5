# datatypes
import json
import yaml
# database connection
import psycopg2
import psycopg2.extras
import psycopg2.extensions as psql_ext
from psycopg2 import sql
# computation
import pandas as pd
# utilities
from pathlib import Path
import os
import itertools
from urllib.request import urlopen
# typing
from typing import Union

def drop_recreate_schema(
    conn: psql_ext.connection,
    schema: str
) -> None:
    # drop all tables
    try:
        with conn.cursor() as cur:
            # Fetch all table names in the schema
            cur.execute(sql.SQL("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE';
            """), [schema])
            
            tables = cur.fetchall()
            
            if tables:
                # Drop each table
                for table in tables:
                    table_name = table[0]
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name)
                    ))
                    print(f"Table '{table_name}' dropped.")
            
            # Commit the changes
            conn.commit()
            
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    # drop recreate schema
    try:
        with conn.cursor() as cur:
            # drop recreate
            print(f"All tables in {schema} dropped successfully.")
            cur.execute(f'DROP SCHEMA IF EXISTS {schema}')
            print(f"Dropped Schema {schema}.")
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
            print(f"Created Schema {schema}.")
            # Commit the changes
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

def read_yaml_to_dict(yaml_file: Union[str, Path]) -> dict:
    """Reads a YML file to a dictionary"""
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return data

def generate_create_table_statement(schema:str, schema_data:dict) -> str:
    table_name = schema_data['name']
    columns = schema_data['columns']
    
    column_definitions = []
    for column in columns:
        column_definitions.append(f"{column['name']} {column['type']}")
    
    columns_str = ",\n  ".join(column_definitions)
    create_table_statement = f"CREATE TABLE {schema}.{table_name} (\n  {columns_str}\n);"
    
    return create_table_statement

def drop_recreate_table(
    *,
    db_schema: str,
    table_schema: dict,
    conn: psql_ext.connection
) -> None:
    try:
        with conn.cursor() as cursor:
            print(f'Dropping {db_schema}.{table_schema["name"]}')
            drop_statement = f'DROP TABLE IF EXISTS {db_schema}.{table_schema["name"]}'
            cursor.execute(drop_statement)
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    try:
        with conn.cursor() as cursor:
            print(f'Creating {db_schema}.{table_schema["name"]}')
            create_statement = generate_create_table_statement(db_schema, table_schema)
            cursor.execute(create_statement)
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

def upload_dataframe(
    *,
    conn: psql_ext.connection,
    dataframe: pd.DataFrame,
    db_schema: str,
    table_name: str,
) -> None:
    data_dict = dataframe.to_dict(orient='records')
    columns = data_dict[0].keys()
    query = 'INSERT INTO {}.{} ({}) VALUES %s'.format(
        db_schema,
        table_name,
        ','.join(columns)
    )
    values = [[value for value in data.values()] for data in data_dict]
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
            conn.commit()
            print(f"Uploaded {len(dataframe)} records to {db_schema}.{table_name}")
    except Exception as e:
        print(f"Error uploading to {db_schema}.{table_name}:\n")
        print(f"Error: {e}")
        conn.rollback()