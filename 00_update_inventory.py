#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
This script automates the process of collecting and consolidating SQL Server and OLAP database information.
It retrieves server lists from Active Directory, categorizes them, retrieves database details from SQL Server and OLAP instances,
merges the data, and uploads it to a central SQL Server database.
The script also handles error reporting for connection failures.

Further improvements (Optional):


    TODO: (DONE) Configuration: Consider using a configuration file (e.g., .ini, .yaml, .json) to store constants like file paths, server names, and connection details. This makes the script more configurable without modifying the code.
    TODO: Error Handling: You have good error handling for pyodbc.Error. Consider adding more specific error handling for file operations (e.g., FileNotFoundError, PermissionError) and pandas operations.
    Logging: Instead of just print statements, consider using the logging module for more robust logging. You can configure different logging levels (e.g., INFO, WARNING, ERROR) and log to a file.
    Modularity: If queries.py and exceptions.py grow large, consider breaking them down into smaller, more focused modules.
    Dependencies: Consider using a requirements.txt file to explicitly list all dependencies (e.g., pyodbc, pandas, tqdm). This makes it easier to reproduce the environment.
    Testing: Ideally, add unit tests to verify the functionality of individual functions.

"""

import pyodbc
import time
from datetime import datetime
import subprocess
import os.path
from sys import path
import pandas as pd
from tqdm import tqdm
import json

# Import custom modules
from queries.queries import *

path.append('\\Program Files\\Microsoft.NET\\ADOMD.NET\\160')

testing = False
conn_error = []

# Load configuration from JSON file
CONFIG_FILE = 'config/config.json'  # Define the config file name

# Handling possible json file errors
try:
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_FILE}' not found.  Exiting.")
    exit(1)  # Exit the script if the config file is not found
except json.JSONDecodeError:
    print(f"Error: Invalid JSON format in '{CONFIG_FILE}'.  Exiting.")
    exit(1)  # Exit if there's a JSON decoding error

# Extract settings from the config
AD_SERVERS_FILE = config['file_paths']['ad_servers_file']
CLUSTER_FILE = config['file_paths']['cluster_file']
NON_CLUSTER_FILE = config['file_paths']['non_cluster_file']
SERVERS_TO_BE_REVIEWED_FILE = config['file_paths']['servers_to_be_reviewed_file']
OPERATIONAL_SERVERS_FILE = config['file_paths']['operational_servers_file']
SSAS_TAB_SERVERS_FILE = config['file_paths']['ssas_olap_servers_file']
DATABASES_FILE = config['file_paths']['databases_file']
INSERT_STATEMENTS_FILE = config['file_paths']['insert_statements_file']
MERGED_CSV_FILE = config['file_paths']['merged_csv_file']
CONNECTION_FAILURES_FILE = config['file_paths']['connection_failures_file']
DATABASE_TXT_PATH = config['file_paths']['database_txt_path']
DATABASE_CSV_PATH = config['file_paths']['database_csv_path']
SERVER_DETAILS_TXT_PATH = config['file_paths']['server_details_txt_path']
SERVER_DETAILS_CSV_PATH = config['file_paths']['server_details_csv_path']
MERGED_DATASETS_CSV_PATH = config['file_paths']['merged_datasets_csv_path']

TARGET_SQL_SERVER = config['server_details']['target_sql_server']
TABLE_NAME_TESTING = config['server_details']['table_name_testing']
TABLE_NAME = config['server_details']['table_name']

CLUSTER_KEYWORD = config['server_details']['cluster_keyword']

serversAwaitingReview = config['servers']['servers_to_be_reviewed']
olap_servers = config['servers']['olap_servers']
servers_to_exclude = config['servers']['servers_to_exclude']

ip_veeam = config['servers']['ip_address_veeam']
server_starpren = config['servers']['server_name']


def execution_time():
    """Print the elapsed time since the start of the script."""
    end_time = time.time()
    elapsed_time = end_time - start_time

    if elapsed_time >= 60:
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        print(f"\nExecution time: {minutes} min {seconds:.4f} seconds")
    else:
        print(f"\nExecution time: {elapsed_time:.4f} seconds")


def retrieve_ad_servers():
    """Retrieve servers from Active Directory and save them to a file."""
    print('\nRetrieving servers from Active Directory...')
    # Define the PowerShell query to retrieve server names, OS, and distinguished names
    ad_query = (
        "Get-ADComputer -Filter \"Name -like 'SR*'\" -Properties Name, OperatingSystem, DistinguishedName | "
        "Where-Object { $_.DistinguishedName -like \"*OU=Bases de datos,OU=Servidores,OU=Equipos Mutua,DC=mutua,DC=es*\" } | "
        "Select-Object Name | Format-Table -AutoSize -HideTableHeaders"
    )
    # Execute the PowerShell command
    powershell_result = subprocess.run(
        ["powershell", "-Command", ad_query], capture_output=True, text=True
    )
    # Handle errors during PowerShell execution
    if powershell_result.returncode != 0:
        print(f"Error executing PowerShell command: {powershell_result.stderr}")
        return []

    # Process the output
    server_data_raw = powershell_result.stdout.strip().split('\n')
    ad_servers = [server.strip().upper() for server in server_data_raw if server.strip()]

    # Write the server names to a file
    with open(AD_SERVERS_FILE, mode='w') as ad_servers_file:
        for server in ad_servers:
            ad_servers_file.write(server + '\n')

    with open(AD_SERVERS_FILE, mode='a') as ad_servers_file:
        lines_to_write = [
            f'{ip_veeam}\n',
            f'{server_starpren}\n'
        ]
        ad_servers_file.writelines(lines_to_write)

    print('Task completed.')
    print(f"\nTotal servers {len(ad_servers)}\nFile path: {AD_SERVERS_FILE}")
    return ad_servers


def categorize_ad_servers():
    """Identify cluster and non-cluster objects from AD OU servers file."""
    # Read server names from the Active Directory servers file
    with open(AD_SERVERS_FILE) as file:
        ad_servers = file.readlines()

    # Categorize servers based on whether they are cluster objects or not
    cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD in item]
    non_cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD not in item]
    operational_servers = [item.strip().upper() for item in non_cluster_objects if
                           item not in serversAwaitingReview and item not in olap_servers and item not in servers_to_exclude]

    # Write cluster objects to a file
    print(f"\nCluster objects: {len(cluster_objects)}\nFile path: {CLUSTER_FILE}")
    with open(CLUSTER_FILE, mode='w') as cluster_file:
        for cluster_object in cluster_objects:
            cluster_file.write(cluster_object + '\n')

    # Write non-cluster objects to a file
    print(f"\nAD OU non-cluster servers: {len(non_cluster_objects)}\nFile path: {NON_CLUSTER_FILE}")
    with open(NON_CLUSTER_FILE, mode='w') as non_cluster_file:
        for non_cluster_object in non_cluster_objects:
            non_cluster_file.write(non_cluster_object + '\n')

    # Write operational servers to a file
    print(f"\nOperational servers: {len(operational_servers)}\nFile path: {OPERATIONAL_SERVERS_FILE}")
    with open(OPERATIONAL_SERVERS_FILE, mode='w') as opt_servers:
        for i in operational_servers:
            opt_servers.write(i + '\n')

    # 'olap_servers' is not defined
    print(f"\nAnalysis and tabular servers: {len(olap_servers)}\nFile path: {SSAS_TAB_SERVERS_FILE}")
    with open(SSAS_TAB_SERVERS_FILE, mode='w') as olap_data:
        for olap_items in olap_servers:
            olap_data.write(olap_items + '\n')

    return operational_servers


def retrieve_dbs(servers):
    """Retrieve databases from SQL servers and save them to a file."""

    print('\nConnecting to SQL Server.\nGetting current databases..\nSaving to a file...')
    with open(DATABASES_FILE, mode='w') as dbs:
        for server in tqdm(servers, desc="Retrieving databases", ascii=" |"):
            try:
                # Establish a connection to the SQL Server
                conn = pyodbc.connect(
                    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=Yes'
                )
                cursor = conn.cursor()
                # Execute the query to retrieve user databases
                database_rows = cursor.execute(user_db_list_query).fetchall()
                # Iterate over the results and write them to the file

                if not database_rows:
                    dbs.writelines(f"{server},No database found\n")
                else:
                    for row in database_rows:
                        x = f"{server},{row.name}"
                        dbs.writelines(x + '\n')

            except pyodbc.Error as e:
                conn_error.append(f"{server} - ODBC Error - {str(e)}")
            except ConnectionError as e:
                conn_error.append(f"{server} - Connection Error- {str(e)}")
            except Exception as e:
                conn_error.append(f"{server} - Unexpected Error - {str(e)}")

    with open(CONNECTION_FAILURES_FILE, "w", encoding='utf-8') as cnx_fails:  # Changed file name to match constant
        for k in conn_error:
            cnx_fails.write(f"{k}\n")


def olap_databases():
    """Retrieve OLAP databases and append them to the databases file."""
    with open(SSAS_TAB_SERVERS_FILE) as data:
        servers_list = data.readlines()

    # Remove blank lines and strip newline characters
    servers_list = [server.strip() for server in servers_list if server.strip()]

    from pyadomd import Pyadomd

    # Open the existing file in append mode
    with open(DATABASES_FILE, 'a') as output_file:
        for server in tqdm(servers_list, desc="Retrieving OLAP databases", ascii=" |"):
            conn_str = f"Provider=MSOLAP;Data Source={server};Integrated Security=SSPI;"

            try:
                # Use 'with' statement to automatically manage connection lifecycle
                with Pyadomd(conn_str) as conn:
                    # SQL query
                    olap_query = "SELECT * FROM $SYSTEM.DBSCHEMA_CATALOGS"

                    # Execute query and fetch results
                    with conn.cursor().execute(olap_query) as cursor:
                        databases = cursor.fetchall()

                    if not databases:
                        output_file.write(f"{server},No database found\n")
                    else:
                        # Print results
                        for db in databases:
                            output_file.write(f"{server},{db[0]}\n")

            except pyodbc.Error as e:
                conn_error.append(f"{server}\n: ODBC Error - {str(e)}")
            except ConnectionError as e:
                conn_error.append(f"{server} - Connection Error-  {str(e)}")
            except Exception as e:
                conn_error.append(f"{server} - Unexpected Error - {str(e)}")

    with open(CONNECTION_FAILURES_FILE, "a", encoding='utf-8') as cnx_fails:  # Changed file name to match constant
        for k in conn_error:
            cnx_fails.write(f"{k}\n")


def retrieve_servers_info():
    """Retrieve server details and save them to a file."""
    with open(SERVER_DETAILS_TXT_PATH, mode='w') as servers_info:  # Changed file name to match constant
        try:
            # Establish a connection to the target SQL Server
            target_conn = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={TARGET_SQL_SERVER};Trusted_Connection=Yes'
            )
            target_conn_cursor = target_conn.cursor()
            # Execute the query to retrieve server details
            records_set = target_conn_cursor.execute(distinct_server_details)
            # Iterate over the results and write them to the file
            for element in records_set:
                full_info = f"{element.ServerName},{element.RespContact1},{element.Email1},{element.RespContact2},{element.Email2},{element.Env},{element.SQLVersion},{element.InstanceType},{element.Lstnr},{element.BackupRetDays},{element.ServiceDesk},{element.RelAppServ},{element.Comments},{element.Maintenance}"

                servers_info.writelines(full_info + '\n')

        except pyodbc.Error:
            print(f'ERROR: Cannot establish connection with {TARGET_SQL_SERVER}.\nPlease, check!')


def convert_to_csv(txt_path, csv_path):
    """Convert a text file to CSV format, handling potential field issues."""
    try:
        data = pd.read_csv(txt_path, delimiter=',', encoding='latin-1', header=None, skipinitialspace=True)
        data.to_csv(csv_path, index=False, header=False)
    except pd.errors.ParserError as e:
        print(f"Error converting {txt_path} to CSV: {e}")
        print("Check for inconsistent delimiters or extra commas in the file.")
        raise


def merge_datasets():
    """Merge the databases and server details datasets into a single dataset."""
    # Load the datasets
    df_databases = pd.read_csv(DATABASE_CSV_PATH, names=['ServerName', 'DatabaseName'],
                               skipinitialspace=True)  # Updated to use constant
    df_server_details = pd.read_csv(SERVER_DETAILS_CSV_PATH,  # Updated to use constant
                                    names=['ServerName', 'RespContact1', 'Email1', 'RespContact2', 'Email2', 'Env',
                                           'SQLVersion', 'InstanceType', 'Lstnr', 'BackupRetDays', 'ServiceDesk',
                                           'RelAppServ',
                                           'Comments', 'Maintenance'], skipinitialspace=True)

    # Handle potential missing values in 'BackupRetDays' before the merge
    df_server_details['BackupRetDays'] = df_server_details['BackupRetDays'].astype(str).str.split('.').str[0]

    # Perform the merge
    merged_df = pd.merge(df_databases, df_server_details, on='ServerName', how='left')

    # Optionally, save the merged dataframe to a new CSV file
    merged_df.to_csv(MERGED_CSV_FILE, index=False)  # Updated to use constant


def read_csv_and_insert_data(file_path):
    """Read CSV and inserts to SQL Server"""
    if testing:
        # use testing table
        table_name_check = TABLE_NAME_TESTING
    else:
        table_name_check = TABLE_NAME

    truncate_table_query = f"TRUNCATE TABLE {table_name_check}"

    # Read the CSV file
    df = pd.read_csv(file_path)

    # Add GeneratedDateTime column
    current_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    df.insert(0, "GeneratedDateTime", current_datetime)

    # Generate and save INSERT statements
    insert_statements = []
    with open(INSERT_STATEMENTS_FILE, 'w') as i:
        for _, row in df.iterrows():
            columns = ', '.join(df.columns)
            values = []
            for col, val in zip(df.columns, row):
                if pd.isna(val):
                    values.append("''")
                elif col == "BackupRetDays":  # Handle BackupRetDays specifically
                    values.append(str(int(float(val))) if pd.notna(val) else "''")
                else:
                    values.append("'" + str(val).replace("'", "''") + "'")
            values_str = ', '.join(values)
            insert_statement = f"INSERT INTO {table_name_check} ({columns}) VALUES ({values_str});"
            i.write(insert_statement + '\n')
            insert_statements.append(insert_statement)

    print(f"\nInsert statements: {len(insert_statements)}\nFile path: {INSERT_STATEMENTS_FILE}")

    # Execute INSERT statements on SQL Server
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={TARGET_SQL_SERVER};Trusted_Connection=Yes"

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Truncate table before inserting data
    print(f'\nTable name: {table_name_check}')
    print(f'Table truncated.')
    cursor.execute(truncate_table_query)

    # Inserting data
    for statement in tqdm(insert_statements, desc='Inserting in progress...', ascii=" |"):
        cursor.execute(statement)

    conn.commit()
    print("All INSERT statements have been executed successfully.")


def execute_tasks():
    """Execute all tasks in the script."""
    # Check if the AD servers file exists, if not, retrieve the servers from Active Directory
    if not os.path.exists(AD_SERVERS_FILE):
        retrieve_ad_servers()
    else:
        print(f"\nReading data from file {AD_SERVERS_FILE}")

    # Categorize the AD servers into cluster and non-cluster objects
    valid_servers = categorize_ad_servers()
    # Retrieve databases from SQL Servers
    retrieve_dbs(servers=valid_servers)
    # Retrieve OLAP databases
    olap_databases()
    # Retrieve server information
    retrieve_servers_info()

    # Convert the database and server details text file to CSV format
    print('\nCreating and merging datasets...\nSaving datasets to files...')

    convert_to_csv(DATABASE_TXT_PATH, DATABASE_CSV_PATH)  # Updated to use constant
    convert_to_csv(SERVER_DETAILS_TXT_PATH, SERVER_DETAILS_CSV_PATH)  # Updated to use constant
    # Saving csv
    print(f"\nDatabases dataset (.csv)\nFile path: {DATABASE_CSV_PATH}")  # Updated to use constant
    print(f"\nServers details dataset (.csv)\nFile path: {SERVER_DETAILS_CSV_PATH}")  # Updated to use constant
    print(f"\nMerged datasets (.csv)\nFile path: {MERGED_DATASETS_CSV_PATH}")  # Updated to use constant
    merge_datasets()

    # Read merged dataset
    read_csv_and_insert_data(file_path=MERGED_CSV_FILE)  # Updated to use constant

    reporting()
    print('\nTask(s) completed successfully!')


def reporting():
    """Report servers with connection errors."""
    # Check if there were any connection failures
    with open(CONNECTION_FAILURES_FILE, mode='r', encoding='utf-8') as reporting_fails:  # Updated to use constant
        r = reporting_fails.readlines()
    if r:
        print('\nAn error occurred while connecting to the following server(s):')
        # Print the list of servers that failed to connect
        for failed_server in r:
            print(failed_server)


if __name__ == "__main__":
    # Record the start time of the script
    start_time = time.time()
    # Execute the main tasks
    if testing:
        print(f'\n....T E S T I N G   M O D E....')
    execute_tasks()
    # Print the execution time of the script
    execution_time()
