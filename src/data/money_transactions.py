import psycopg2
from psycopg2 import sql
from config import config

def transfer_money(from_account, to_account, amount):
    """Transferring money from one account to another using a transaction."""
    try:
        # Using 'with' for automatic connection and cursor handling
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                # Begin transaction
                con.autocommit = False  # Disable autocommit to start a transaction
                deduct_sql = "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s;"
                cursor.execute(deduct_sql, (amount, from_account, amount))

                # Check if money was deducted successfully
                if cursor.rowcount == 0:
                    raise ValueError("Insufficient funds or account not found")

                # Step 2: Add money to the second account
                add_sql = "UPDATE accounts SET balance = balance + %s WHERE id = %s;"
                cursor.execute(add_sql, (amount, to_account))

                # Step 3: Commit transaction (both operations succeed)
                con.commit()
                print("Money transferred successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        # Rollback transaction if anything goes wrong
        if con:
            con.rollback()
        print("Transaction rolled back!")


def create_table(table_name: str, columns: dict):
    """
    Creates a new table in the database.
    
    :param table_name: The name of the table to create.
    :param columns: A dictionary where the keys are column names and the values are column definitions.
                    Example: {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(100)', 'age': 'INTEGER'}
    """
    con = None
    cursor = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        # Begin transaction (optional, but recommended)
        con.autocommit = False  # Disable autocommit to control transaction manually

        # Construct the SQL query for creating the table
        columns_sql = ', '.join([f"{column} {definition}" for column, definition in columns.items()])
        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(columns_sql)
        )

        # Execute the query to create the table
        cursor.execute(create_table_sql)

        # Commit the transaction to persist the changes
        con.commit()
        print(f"Table '{table_name}' created successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error creating table: {error}")
        if con:
            con.rollback()  # Rollback in case of an error

    finally:
        # Clean up: close the cursor and connection
        if cursor:
            cursor.close()
        if con:
            con.close()

# Example usage: Create a table named "employees" with "id", "name", and "age"
# columns_definition = {
#     'id': 'SERIAL PRIMARY KEY',
#     'name': 'VARCHAR(100)',
#     'age': 'INTEGER'
# }

# create_table('employees', columns_definition)

# Example usage
#transfer_money(1, 2, 500)
