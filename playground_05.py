"""
This script automates the process of collecting and consolidating SQL Server and OLAP database information.
It retrieves server lists from Active Directory, categorizes them, retrieves database details from SQL Server and OLAP instances,
merges the data, and uploads it to a central SQL Server database.
The script also handles error reporting for connection failures.
"""

import pyodbc
import time
from datetime import datetime
import subprocess
import os.path
from sys import path
import pandas as pd
from tqdm import tqdm

# Import custom modules
from queries.queries import *
from exceptions.exceptions import *

path.append('\\Program Files\\Microsoft.NET\\ADOMD.NET\\160')

# Constants - Define file paths and server names for configuration
AD_SERVERS_FILE = 'update_data/ad_ou_servers.txt'
CLUSTER_FILE = 'update_data/clusters_objects.txt'
NON_CLUSTER_FILE = 'update_data/server_objects.txt'
SERVERS_TO_BE_REVIEWED_FILE = 'update_data/servers_to_be_reviewed.txt'
OPERATIONAL_SERVERS_FILE = 'update_data/operational_servers.txt'
SSAS_TAB_SERVERS_FILE = 'update_data/ssas_olap_servers.txt'
DATABASES_FILE = 'update_data/databases.txt'
INSERT_STATEMENTS_FILE = 'update_data/insert_statements.sql'
MERGE_CSV_FILE = 'update_data/merged_dataset.csv'
CONEX_FAILURES_FILE = 'update_data/conex_failures.txt'

# Global variables to store connection failure information
sqldb_conx_failed = []
olap_conx_fails = []
target_conx_fails = []
fails = sqldb_conx_failed + olap_conx_fails + target_conx_fails

# Define file paths for database and server details
database_txt_path = 'update_data/databases.txt'
database_csv_path = 'update_data/databases.csv'
server_details_txt_path = 'update_data/server_details.txt'
server_details_csv_path = 'update_data/server_details.csv'
merged_datasets_csv_path = 'update_data/merged_dataset.csv'


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
    """
    Retrieve servers from Active Directory and save them to a file.

    Returns:
        list: A list of servers retrieved from Active Directory.
    """
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

    print('Task completed.')
    print(f"\nTotal servers {len(ad_servers)}\nFile path: {AD_SERVERS_FILE}")
    return ad_servers


def categorize_ad_servers():
    """
    Identify cluster and non-cluster objects from AD OU servers file.

    Returns:
        list: A list of operational servers.
    """
    # Read server names from the Active Directory servers file
    with open(AD_SERVERS_FILE) as file:
        ad_servers = file.readlines()

    # Categorize servers based on whether they are cluster objects or not
    cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD in item]
    non_cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD not in item]
    operational_servers = [item.strip().upper() for item in non_cluster_objects if
                           item not in serversAwaitingReview and item not in olap_servers]  # serversAwaitingReview is not defined

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
    """
    Retrieve databases from SQL servers and save them to a file.

    Args:
        servers (list): A list of server names to retrieve databases from.
    """
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

            except pyodbc.Error:
                # Handle connection errors

                sqldb_conx_failed.append(f"{server}")


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

            except pyodbc.Error:
                olap_conx_fails.append(f"{server}")


def retrieve_servers_info():
    """Retrieve server details and save them to a file."""
    with open('update_data/server_details.txt', mode='w') as servers_info:
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
            # Handle connection errors
            target_conx_fails.append(f"{TARGET_SQL_SERVER}")


def convert_to_csv(txt_path, csv_path):
    """
    Convert a text file to CSV format, handling potential field issues.

    Args:
        txt_path (str): Path to the input text file.
        csv_path (str): Path to the output CSV file.
    """
    try:
        data = pd.read_csv(txt_path, delimiter=',', encoding='latin-1', header=None, skipinitialspace=True)
        data.to_csv(csv_path, index=False, header=False)
    except pd.errors.ParserError as e:
        print(f"Error converting {txt_path} to CSV: {e}")
        print("Check for inconsistent delimiters or extra commas in the file.")
        raise


def read_csv_and_insert_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Add GeneratedDateTime column
    current_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    df.insert(0, "GeneratedDateTime", current_datetime)

    # Generate and save INSERT statements
    insert_statements = []
    with open(INSERT_STATEMENTS_FILE, 'w') as f:
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
            insert_statement = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values_str});"
            f.write(insert_statement + '\n')
            insert_statements.append(insert_statement)

    print(f"Insert statements: {len(insert_statements)}\nFile path: {INSERT_STATEMENTS_FILE}")

    # Execute INSERT statements on SQL Server
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={TARGET_SQL_SERVER};Trusted_Connection=Yes"

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Truncate table before inserting data
    print('Table truncated.')
    cursor.execute(truncate_table_query)

    # Inserting data
    for statement in tqdm(insert_statements, desc='Inserting in progress...', ascii=" |"):
        cursor.execute(statement)

    conn.commit()
    print("All INSERT statements executed successfully.")


def merge_datasets():
    """Merge the databases and server details datasets into a single dataset."""
    # Load the datasets
    df_databases = pd.read_csv(database_csv_path, names=['ServerName', 'DatabaseName'], skipinitialspace=True)
    df_server_details = pd.read_csv(server_details_csv_path,
                                    names=['ServerName', 'RespContact1', 'Email1', 'RespContact2', 'Email2', 'Env',
                                           'SQLVersion', 'InstanceType', 'Lstnr', 'BackupRetDays', 'ServiceDesk',
                                           'RelAppServ',
                                           'Comments', 'Maintenance'], skipinitialspace=True)

    # Handle potential missing values in 'BackupRetDays' before the merge
    df_server_details['BackupRetDays'] = df_server_details['BackupRetDays'].astype(str).str.split('.').str[0]

    # Perform the merge
    merged_df = pd.merge(df_databases, df_server_details, on='ServerName', how='left')

    # Optionally, save the merged dataframe to a new CSV file
    merged_df.to_csv('update_data/merged_dataset.csv', index=False)


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

    convert_to_csv(database_txt_path, database_csv_path)
    convert_to_csv(server_details_txt_path, server_details_csv_path)
    # Saving csv
    print(f"\nDatabases dataset (.csv)\nFile path: {database_csv_path}")
    print(f"\nServers details dataset (.csv)\nFile path: {server_details_csv_path}")
    print(f"\nMerged datasets (.csv)\nFile path: {merged_datasets_csv_path}")
    merge_datasets()

    # Read merged dataset
    read_csv_and_insert_data(file_path=MERGE_CSV_FILE)

    reporting()
    print('\nTask(s) completed successfully!')


def reporting():
    """Report servers with connection errors."""
    # Check if there were any connection failures
    if fails:
        print('\nAn error occurred while connecting to the following server(s):')
        # Print the list of servers that failed to connect
        for failed_server in fails:
            print(failed_server)


if __name__ == "__main__":
    # Record the start time of the script
    start_time = time.time()
    # Execute the main tasks
    execute_tasks()
    # Print the execution time of the script
    execution_time()
