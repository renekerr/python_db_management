"""
This Python script connects to multiple SQL Server databases, executes a query to retrieve backup information,
and saves the results to a file. It reads server names from a text file, establishes connections to each server,
executes a predefined query, processes the results, and handles any connection errors.
The script also measures and reports its execution time.

"""

import pyodbc
import time
from queries.queries import backup_validation

start_time = time.time()
results = []


def execution_time():
    """Prints the elapsed time since the start of the script in minutes and seconds if over 60 seconds."""
    end_time = time.time()
    elapsed_time = end_time - start_time

    if elapsed_time >= 60:
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        print(f"\nExecution time: {minutes} min {seconds:.4f} seconds")
    else:
        print(f"\nExecution time: {elapsed_time:.4f} seconds")


# Read server list from file
with open('data_sources/servers.txt') as server_file:
    server_list = [server.strip() for server in server_file if server.strip()]

DATABASE_NAME = 'master'
connection_errors = []
print(f'\nTotal servers: {len(server_list)}')

for server in server_list:
    connection = None
    cursor = None
    try:
        # Establish a connection to the SQL Server database
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={DATABASE_NAME};Trusted_Connection=Yes'
        connection = pyodbc.connect(connection_string)

        cursor = connection.cursor()
        cursor.execute(backup_validation)

        # Process query results
        for row in cursor.fetchall():
            device_name = 'Nativo' if row.devicename is None else row.devicename
            is_copy_only = '1' if row.iscopyonly else '0'
            backup_info = f"{server},{row.dbname},{row.backtype},{device_name},{is_copy_only}"
            print(backup_info)
            results.append(backup_info)

    except pyodbc.Error as e:
        connection_errors.append(f"{server}")

    finally:
        # Close resources
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Report connection errors
if connection_errors:
    print('\nErrors occurred while connecting to the following server(s): ')
    for error in connection_errors:
        print(error)
else:
    print("\nAll connections were successful.")

# Save results to output file
output_file_path = 'output/db_backups_info.txt'
with open(output_file_path, mode='w') as output_file:
    output_file.write("Server,Database,BackupType,DeviceName,IsCopyOnly\n")  # Header
    for result in results:
        output_file.write(f"{result}\n")

execution_time()
