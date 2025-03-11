# -*- coding: utf-8 -*-

import time
import subprocess
import os.path
import pyodbc
from queries.queries import *

# Constants
TARGET_SQL_SERVER = 'SRITSQLPRO'
AD_SERVERS_FILE = 'output/ad_ou_servers.txt'
CLUSTER_FILE = 'output/clusters_objects.txt'
NON_CLUSTER_FILE = 'output/server_objects.txt'
SSAS_OLAP_FILE = 'output/ssas_olap.txt'
SAVED_SERVERS_FILE = 'output/saved_servers.txt'
SERVERS_TO_BE_REVIEWED_FILE = 'data_sources/servers_to_be_reviewed.txt'
CLUSTER_KEYWORD = 'CLU'
RESPONSIBLE_PERSON_NAMES = []
server_contact_env_info = []
output_info = []
table_name = '[ADMINISTRACION].[dbo].[ServerDB_Main_Resp_INFO]'
BACKUP_RETENTION_DAYS = 15
empty_resp_name = len(RESPONSIBLE_PERSON_NAMES)


def print_execution_time():
    """Print the elapsed time since the start of the script."""
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nExecution time: {elapsed_time:.4f} seconds")


def retrieve_ad_servers():
    """Retrieve servers from Active Directory and save them to a file."""
    print('\nRetrieving servers from Active Directory...')
    ad_query = (
        "Get-ADComputer -Filter \"Name -like 'SR*'\" -Properties Name, OperatingSystem, DistinguishedName | "
        "Where-Object { $_.DistinguishedName -like \"*OU=Bases de datos,OU=Servidores,OU=Equipos Mutua,DC=mutua,DC=es*\" } | "
        "Select-Object Name | Format-Table -AutoSize -HideTableHeaders"
    )
    powershell_result = subprocess.run(
        ["powershell", "-Command", ad_query], capture_output=True, text=True
    )
    if powershell_result.returncode != 0:
        print(f"Error executing PowerShell command: {powershell_result.stderr}")
        return []

    server_data_raw = powershell_result.stdout.strip().split('\n')
    ad_servers = [server.strip().upper() for server in server_data_raw if server.strip()]

    with open(AD_SERVERS_FILE, mode='w') as ad_servers_file:
        for server in ad_servers:
            ad_servers_file.write(server + '\n')

    print(f'Task completed. \nServers saved to folder {AD_SERVERS_FILE}.')
    return ad_servers


def read_ad_servers():
    """Read servers from the AD OU file and return total servers found."""
    print("\nReading servers from file...")
    with open(AD_SERVERS_FILE) as data:
        ad_servers = data.readlines()

    print(f'Total servers retrieved: {len(ad_servers)}')
    return ad_servers


def get_servers_to_review():
    """Read servers from the Servers To Be Reviewed file and return the list."""
    with open(SERVERS_TO_BE_REVIEWED_FILE) as data:
        servers_to_review = [item.strip().upper() for item in data.readlines()]

    print(f'Servers to be reviewed: {len(servers_to_review)}')
    return servers_to_review


def categorize_ad_servers():
    """Identify cluster and non-cluster objects from AD OU servers file."""
    with open(AD_SERVERS_FILE) as file:
        ad_servers = file.readlines()

    cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD in item]
    non_cluster_objects = [item.strip().upper() for item in ad_servers if CLUSTER_KEYWORD not in item]

    print(f"Cluster objects: {len(cluster_objects)}")
    print(f"AD OU non-cluster servers: {len(non_cluster_objects)}")

    with open(CLUSTER_FILE, mode='w') as cluster_file:
        for cluster_object in cluster_objects:
            cluster_file.write(cluster_object + '\n')

    with open(NON_CLUSTER_FILE, mode='w') as non_cluster_file:
        for non_cluster_object in non_cluster_objects:
            non_cluster_file.write(non_cluster_object + '\n')

    return cluster_objects, non_cluster_objects


def get_tracked_servers():
    """Retrieve and save tracked servers from the database, including SSAS OLAP servers."""
    try:
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={TARGET_SQL_SERVER};Trusted_Connection=Yes'
        )
        cursor = conn.cursor()

        cursor.execute(get_ssas_olap_servers)
        ssas_olap_servers = [row.ServerName.strip().upper() for row in cursor.fetchall()]

        cursor.execute(fetch_all_distinct_servers)
        all_tracked_servers = [row.ServerName.strip().upper() for row in cursor.fetchall()]

        print(f"SSAS OLAP servers: {len(ssas_olap_servers)}")
        print(f"All tracked servers: {len(all_tracked_servers)}")

        with open(SSAS_OLAP_FILE, mode='w') as data:
            for item in ssas_olap_servers:
                data.write(item + '\n')

        with open(SAVED_SERVERS_FILE, mode='w') as data:
            for item in all_tracked_servers:
                data.write(item + '\n')

        cursor.close()
        conn.close()

        return ssas_olap_servers, all_tracked_servers
    except pyodbc.Error:
        print(f"An error occurred while connecting to the server: {TARGET_SQL_SERVER}")
        return [], []


def find_untracked_servers(non_cluster_objects, tracked_sql_servers, olap_servers, servers_to_review):
    """Find servers that are not tracked in the database."""
    untracked_servers = [
        server for server in non_cluster_objects
        if server not in tracked_sql_servers and server not in olap_servers and server not in servers_to_review
    ]

    if len(untracked_servers) == 0:
        print(f"\nTotal unregistered servers: {len(untracked_servers)}\nNo servers to register.")
        print_execution_time()
        exit()
    else:
        for server in untracked_servers:
            print(server)

    return untracked_servers


def contact_mail_mapping():
    """Map responsible person names to their email addresses."""
    email_addresses = []

    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={TARGET_SQL_SERVER};Trusted_Connection=Yes'

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    print('\nResponsible contact(s)')

    for name in RESPONSIBLE_PERSON_NAMES:
        search_pattern = f"%{name}%"

        cursor.execute(ad_user_query_map_mail, (search_pattern,))

        results = cursor.fetchall()

        if results:
            for row in results:
                print(f"{row.displayName},{row.mail}")
                email_addresses.append(row.mail)
        else:
            print(f"No results found for {name}")

    cursor.close()
    conn.close()

    contact_email_map = list(zip(RESPONSIBLE_PERSON_NAMES, email_addresses))

    return contact_email_map


def get_environment(server_name):
    """Determine the environment based on the server name suffix."""
    suffix_segment = server_name[-3:]
    suffix_to_environment = {
        'D': 'DES',
        'P': 'PRE',
        'E': 'PRO',
        'DES': 'DES',
        'PRE': 'PRE',
        'PRO': 'PRO'
    }

    environment_label = next(
        (suffix_to_environment[suffix] for suffix in suffix_to_environment if suffix in suffix_segment),
        'N/A'
    )

    return environment_label


def execute_tasks():
    """Execute the main tasks of reading, categorizing, and tracking servers."""
    if not os.path.exists(AD_SERVERS_FILE):
        retrieve_ad_servers()

    ad_servers = read_ad_servers()
    cluster_objects, non_cluster_objects = categorize_ad_servers()
    olap_servers, tracked_sql_servers = get_tracked_servers()
    servers_to_review = get_servers_to_review()

    untracked_servers = find_untracked_servers(
        non_cluster_objects,
        tracked_sql_servers,
        olap_servers,
        servers_to_review
    )

    if empty_resp_name == 0 or empty_resp_name != len(untracked_servers):
        print('\nCritical: Responsible contacts data missing!')
        print_execution_time()
        exit()
    else:
        contact_email_map = contact_mail_mapping()

        # Combine server names with contact details
        server_contact_details = [
            [server, name, email]
            for server, (name, email) in zip(untracked_servers, contact_email_map)
        ]

        # Get environments for all untracked servers
        environments = [get_environment(server_name) for server_name in untracked_servers]

        # Append environment to full_info
        for i, environment in enumerate(environments):
            server_contact_details[i].append(environment)

        # Printing environment
        print('\nServer(s) environment:')
        for server_name, environment in zip(untracked_servers, environments):
            print(f'{server_name}: {environment}')

        # Print server details including environment
        print(f"\nServer, contact details and environment:")

        for server, name, email, env in server_contact_details:
            info = f'{server},{name},{email},{env}'
            server_contact_env_info.append(info)
            print(info)

        for server, name, email, env in server_contact_details:
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=Yes'
            connector = None  # Initialize the connection variable
            try:
                connector = pyodbc.connect(conn_str)
                cursor = connector.cursor()
                cursor.execute(sql_server_version)  # Get the SQL Server version
                sql_version = cursor.fetchone()[0]
                cursor.execute(instance_type)  # Determine if the server is part of a cluster or stand-alone
                is_clustered = cursor.fetchone()[0]
                cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # Get the list of user databases
                user_databases = [row[0] for row in cursor.fetchall()]

                if not user_databases:
                    user_databases = ['No user database found']

                output_info.extend([[server, db, name, email, env, sql_version, is_clustered] for db in user_databases])

            except Exception:
                print(f"Failed to connect to {server}")
                output_info.append([server, 'Connection failed', name, email, 'Unknown version', 'Unknown status'])

            finally:
                if connector:
                    connector.close()

        for info in output_info:
            print(', '.join(info))

        # Print the INSERT commands
        print()
        print(output_info)
        print('\nCommands to execute')
        for info in output_info:
            server, db, name, email, env, sql_version, sa_hadr = info
            insert_command = (
                f"INSERT INTO {table_name} (ServerName, DatabaseName, RespContact1, Email1, RespContact2, Email2, Env, "
                f"SQLVersion, InstanceType, Lstnr, BackupRetDays, ServiceDesk, RelAppServ, Comments, Maintenance) "
                f"VALUES ('{server}', '{db}', '{name}', '{email}', '', '', '{env}', '{sql_version}', '{sa_hadr}', '', "
                f"{BACKUP_RETENTION_DAYS}, '', '', '', '');"
            )
            print(insert_command)

    return (
        ad_servers,
        cluster_objects,
        non_cluster_objects,
        olap_servers,
        tracked_sql_servers,
        servers_to_review,
        untracked_servers,
        environments
    )


# Main execution block
if __name__ == "__main__":
    start_time = time.time()
    execute_tasks()
    print_execution_time()
