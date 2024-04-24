import os
import pandas as pd
import psycopg2
import psycopg2.extras  # Explicit import for batch operations
import re
import csv

def set_schema_search_path(conn, schema='CRDB'):
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO %s, public;", (schema,))
        conn.commit()

def clean_column_name(column_name):
    """Clean column names to ensure they are valid SQL identifiers."""
    return re.sub(r'\W+', '_', column_name).strip('_').lower()

def guess_data_types(df):
    """Guess column data types based on their contents."""
    column_types = {}
    for col in df.columns:
        if col in ['state', 'county', 'tract', 'block', 'block_geoid']:
            column_types[col] = f'VARCHAR({df[col].astype(str).map(len).max()})'
        else:
            column_types[col] = 'DECIMAL'
    return column_types

def create_table_sql_script(table_name, column_types, filename, conn):
    """Generate and append CREATE TABLE SQL statement to a script."""
    columns_sql = ', '.join([f'"{col}" {typ}' for col, typ in column_types.items()])
    ddl = f'CREATE TABLE IF NOT EXISTS "CRDB".{table_name} ({columns_sql});'  # Specify the schema
    with open(filename, 'w') as sql_script:
        sql_script.write(ddl + "\n")
    cursor = conn.cursor()
    cursor.execute(ddl)
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON "CRDB".{table_name}(state);')
    conn.commit()

def import_data_to_table(df, table_name, state, conn, filename):
    """Import data to the PostgreSQL table and log the action in a SQL script."""
    df['state'] = state
    columns = ', '.join([f'"{col}"' for col in df.columns])
    values_placeholders = ', '.join(['%s' for _ in df.columns])
    insert_sql = f'INSERT INTO "CRDB".{table_name} ({columns}) VALUES ({values_placeholders});'
    with conn.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor, insert_sql, df.values.tolist())
        conn.commit()
    with open(filename, 'a') as sql_script:
        sql_script.write(f"-- Data from state code {state} appended to {table_name}\n")

def convert_txt_to_csv_and_import(directory, conn_params, sql_script_path, table_name="address_data_national"):
    """Process TXT files and import the data into a unified PostgreSQL table."""
    conn = psycopg2.connect(**conn_params)
    set_schema_search_path(conn)
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            state_code = filename[:2]
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath, delimiter='|', dtype={'STATE': 'string', 'COUNTY': 'string', 'TRACT': 'string', 'BLOCK': 'string', 'BLOCK_GEOID': 'string'})
            print(df.head(2), "\n")
            df.columns = [clean_column_name(col) for col in df.columns]
            column_types = guess_data_types(df)
            create_table_sql_script(table_name, column_types, sql_script_path, conn)
            import_data_to_table(df, table_name, state_code, conn, sql_script_path)
    conn.close()

# Set up connection parameters
conn_params = {
    "dbname": 'CRDB',
    "user": 'postgres',
    "password": os.getenv('PostgreSQL_PWD'),
    "host": '127.0.0.1',
    "port": '5432'
}

# Define the directory and SQL script path
directory = '../data/ACLF_AddressCountListingFiles2020_AllStates'
sql_script_path = 'import_aclf_data.sql'
open(sql_script_path, 'w').close()  # Clear the script file before starting

convert_txt_to_csv_and_import(directory, conn_params, sql_script_path)
