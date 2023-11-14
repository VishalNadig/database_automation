from sqlalchemy import create_engine, Table, Column, Integer, MetaData, inspect, text
from sqlalchemy_utils import create_database, database_exists, drop_database
import pandas as pd


USER = "root"
PASSWORD = "A1B2C3D4E5"
HOSTNAME = "localhost"
METADATA = MetaData()
URL = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOSTNAME}"

def create_database_function(database: str):
    """
    Create a database function.

    Args:
        database (str): The name of the database to be created.

    Returns:
        dict: A dictionary containing the status of the database creation. If the database is created successfully, the dictionary will have the key "Ok" and the value "Database Created!". If the database already exists, the dictionary will have the key "Already Exists!" and the value "Database Already Exists!".
        tuple: If an exception occurs during the creation of the database, a tuple with None as the first element and the exception as the second element will be returned.
    """
    try:
        engine = create_engine(URL)
        connection = engine.connect()
        if not database_exists(URL + f"/{database}"):
            connection.execute(create_database(URL + f"/{database}"))
            return {"Ok": "Database Created!"}
        return {"Already Exists!": "Database Already Exists!"}
    except Exception as exception:
        return (None, exception)

def delete_database_function(database: str):
    """
    Deletes a database with the given name.

    Args:
        database (str): The name of the database to be deleted.

    Returns:
        dict or Exception: A dictionary with the keys "Ok" and "Database Deleted!"
        if the database is successfully deleted.
        If the database doesn't exist, a dictionary with the keys "Doesn't Exist!"
        and "Database doesn't Exist!" is returned.
        If an exception occurs during the deletion process, the exception object is returned.
    """
    try:
        with create_engine(URL).connect() as connection:
            if database_exists(URL + f"/{database}"):
                connection.execute(drop_database(URL + f"/{database}"))
                return {"Ok": "Database Deleted!"}
            else:
                return {"Doesn't Exist!": "Database doesn't Exist!"}
    except Exception as exception:
        return exception

def create_tables(database: str, *table_names: str):
    try:
        engine = create_engine(URL + f'/{database}', echo=True)
        inspector = inspect(engine)
        
        if database_exists(URL + f'/{database}'):
            for table_name in table_names:
                if table_name not in inspector.get_table_names():
                    table = Table(table_name, METADATA, Column('Id', Integer, primary_key=True))
                    table.create(bind=engine)
                else:
                    return 'Table already exists!'
        else:
            return {'Error!': 'Database does not exist!'}
    except Exception as e:
        return e

def delete_tables(database: str, *table_names: str):
    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        
        if not database_exists(URL + f"/{database}"):
            return "database doesn't exist!"
        
        tables_to_delete = [table for table in table_names if table in inspector.get_table_names()]
        
        if not tables_to_delete:
            return "Tables don't exist!"
        
        metadata = MetaData(bind=engine)
        for table_name in tables_to_delete:
            table = Table(table_name, metadata, autoload=True, autoload_with=engine)
            table.drop(bind=engine)
        
    except Exception as exception:
        return exception

def insert_columns(database: str, table_name: str, column_name: str, datatype: str, size: str, command: str):
    try:
        engine = create_engine(URL + f'/{database}')
        inspector = inspect(engine)
        command = command.lower()
        
        if database_exists(URL + f'/{database}') and table_name in inspector.get_table_names():
            command = text(f'ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {command}')
            engine.execute(command)
            return 'Successfully inserted columns!'
    except Exception as exception:
        return exception

def delete_columns(database: str, table_name: str, column_name: str):
    """
    Deletes a column from a table in a specified database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column to be deleted.

    Returns:
        dict or Exception: If successful, returns an empty dictionary. If the table or database does not exist, returns an error dictionary. If there is an exception, returns the exception object.

    Raises:
        None
    """

    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)

        if not database_exists(URL + f"/{database}"):
            return {"Error": "Database does not exist!"}

        if table_name not in inspector.get_table_names():
            return {"Error": "Table does not exist!"}

        command = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
        engine.execute(command)
    except Exception as exception:
        return exception

    return {}

def modify_column(database: str, table_name: str, column_name: str, command: str):
    """
    Modifies a column in a specified table of a given database.

    Parameters:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column to be modified.
        command (str): The modification command to be executed.

    Returns:
        str: A message indicating the status of the modification.
            - If the table doesn't exist, returns "table doesn't exist".
            - If the database doesn't exist, returns "Database doesn't exist".
            - If an exception occurs, returns the exception message.
    """
    try:
        with create_engine(URL+f"/{database}").connect() as connection:
            inspector = inspect(connection)
            if database_exists(URL+f"{database}"):
                if table_name in inspector.get_table_names():
                    command = f"ALTER TABLE {table_name} MODIFY {column_name} {command}"
                    connection.execute(command)
                else:
                    return "table doesn't exist"
            else:
                return "Database doesn't exist"
    except Exception as exception:
        return str(exception)

from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.exc import NoSuchTableError, NoSuchDatabaseError

def inspect_columns(database: str, table: str, *column: str):
    """
    Inspects the columns of a database table.

    Args:
        database (str): The name of the database.
        table (str): The name of the table.
        *column (str): Variable number of column names.

    Returns:
        list or str: If no column names are provided, returns a list of dictionaries representing the columns of the table.
                     If column names are provided, returns the selected columns as a list of tuples.
                     If the table does not exist, returns the string "Table doesn't Exist!".
                     If the database does not exist, returns the string "Database doesn't exist".
                     If an exception occurs, returns the exception object.

    Raises:
        NoSuchDatabaseError: If the specified database does not exist.
        NoSuchTableError: If the specified table does not exist.
    """
    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        
        if inspector.has_table(table):
            if not column:
                return inspector.get_columns(table)
            else:
                return engine.execute(f"SELECT {', '.join(column)} FROM {table}").fetchall()
        else:
            return "Table doesn't Exist!"
    except NoSuchDatabaseError:
        return "Database doesn't exist"
    except NoSuchTableError:
        return "Table doesn't Exist!"
    except Exception as exception:
        return exception

def query(database: str, table_name: str, filter_condition: str):
    """
    Executes a SQL query on a specified database table with a filter condition.

    Args:
        database (str): The name of the database to query.
        table_name (str): The name of the table to query.
        filter_condition (str): The filter condition to apply to the query.

    Returns:
        Exception or None: If an error occurs during the query execution, the exception is returned. Otherwise, None is returned.
    """
    try:
        with create_engine(URL + f"/{database}").connect() as connection:
            inspector = inspect(connection)
            if database_exists(URL + f"/{database}") and table_name in inspector.get_table_names():
                return connection.execute(f"SELECT * FROM {table_name} WHERE {filter_condition}").fetchall()
    except Exception as exception:
        return exception
    


def check_for_duplicates(database: str, table_name: str, column_name: str):
    """
    Checks for duplicates in a specified database table column.
    """
    with create_engine(URL + f"/{database}").connect() as connection:
        inspector = inspect(connection)
        if database_exists(URL + f"/{database}") and table_name in inspector.get_table_names():
            return connection.execute(f"SELECT {column_name} FROM {table_name} GROUP BY {column_name} HAVING COUNT({column_name}) > 1").fetchall()



def delete_duplicates(dataframe: pd.DataFrame):
    """
    Delete duplicates from the given dataframe.

    Args:
        dataframe (pd.DataFrame): The dataframe to remove duplicates from.

    Returns:
        pd.DataFrame: The dataframe with duplicates removed.
    """
    return dataframe.drop_duplicates()
    

if __name__ == '__main__':
    check_for_duplicates("food_db", "food_recipes".upper(), "RECIPE_NAME")