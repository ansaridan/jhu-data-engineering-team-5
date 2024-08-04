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

def generate_create_table_statement(schema: str, table_name: str, schema_data: dict) -> str:
    columns = schema_data['columns']
    
    column_definitions = []
    for column in columns:
        column_definitions.append(f"{column['name']} {column['type']}")
    
    columns_str = ",\n  ".join(column_definitions)
    
    # Add primary key clause if primary_key is present in schema_data
    if 'primary_key' in schema_data:
        primary_key_cols = ", ".join(schema_data['primary_key'])
        primary_key_clause = f",\n  PRIMARY KEY ({primary_key_cols})"
    else:
        primary_key_clause = ""
    
    create_table_statement = f"CREATE TABLE {schema}.{table_name} (\n  {columns_str}{primary_key_clause}\n);"
    
    return create_table_statement


def drop_recreate_table(
    *,
    db_schema: str,
    table_name: str,
    table_schema: dict,
    conn: psql_ext.connection
) -> None:
    try:
        with conn.cursor() as cursor:
            print(f'Dropping {db_schema}.{table_name}')
            drop_statement = f'DROP TABLE IF EXISTS {db_schema}.{table_name}'
            cursor.execute(drop_statement)
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    try:
        with conn.cursor() as cursor:
            print(f'Creating {db_schema}.{table_name}')
            create_statement = generate_create_table_statement(db_schema, table_name, table_schema)
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
    table_schema: dict,
) -> None:
    columns = [col["name"] for col in table_schema["columns"]]
    data_dict = dataframe[columns].to_dict(orient='records')
    
    # Prepare the ON CONFLICT clause if primary_key is present
    if "primary_key" in table_schema:
        primary_key_cols = table_schema["primary_key"]
        conflict_target = ','.join(primary_key_cols)
        update_cols = [col for col in columns if col not in primary_key_cols]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        conflict_clause = f" ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}"
    else:
        conflict_clause = ""
    
    query = 'INSERT INTO {}.{} ({}) VALUES %s{}'.format(
        db_schema,
        table_name,
        ','.join(columns),
        conflict_clause
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
