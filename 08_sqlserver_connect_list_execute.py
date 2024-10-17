"""
This script performs the following tasks:
1. Reads a list of SQL Server instances from a file.
2. Connects to each server using pyodbc.
3. Executes a query to retrieve user database information.
4. Prints and stores the results for each server.
5. Handles connection errors and logs problematic servers.
6. Saves the collected data to an output file.
7. Measures and reports the total execution time.

The script is useful for quickly gathering an inventory of user databases
across multiple SQL Server instances in an environment.
"""


import pyodbc
import time
from queries.queries import *

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


with open('data_sources/servers.txt') as data:
    servers_check = data.readlines()

# Remove blank lines and strip newline characters
servers_check = [server.strip() for server in servers_check if server.strip()]

database_name = 'master'
connection_error = []
print(f'\nTotal servers: {len(servers_check)}\n')

for server in servers_check:
    connection = None
    database_cursor = None
    try:
        # Establish a connection to the SQL Server database
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database_name};Trusted_Connection=Yes'
        )

        # Create a cursor object to interact with the database
        database_cursor = connection.cursor()

        # Execute the SQL query
        database_cursor.execute(user_db_list_query)

        # Fetch all rows from the executed query
        user_databases = database_cursor.fetchall()

        if not user_databases:
            # If no user databases found, add to no_user_db list
            print(f"{server},No user databases")
            results.append(f"{server},No user databases")
        else:
            # Iterate over the rows and print each one
            for row in user_databases:
                all_info = f"{server},{row.name}"
                print(all_info)
                results.append(all_info)

    except pyodbc.Error as e:
        connection_error.append(f"{server}")

    finally:
        # Ensure resources are properly closed, even if an exception occurs
        if database_cursor:
            database_cursor.close()
        if connection:
            connection.close()

# Connection error listing
if connection_error:
    print('\nAn error occurred while connecting to the following server(s): ')
    for error in connection_error:
        print(f"{error}")
else:
    print("\nAll connections were successful.")

# Save results to output file
with open('output/userdb_by_server.txt', mode='w') as d:
    d.write("Server,Name\n")  # Header
    for result in results:
        d.write(f"{result}\n")

execution_time()
