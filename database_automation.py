from sqlalchemy import create_engine, Table, Column, Integer, MetaData, inspect, text
from sqlalchemy_utils import create_database, database_exists, drop_database

USER = "root"
PASSWORD = "MYmac123"
HOSTNAME = "localhost"
METADATA = MetaData()
URL = f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}"

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
        if not database_exists(URL+f"/{database}"):
            connection.execute(create_database(URL+f"/{database}"))
            return {"Ok": "Database Created!"}
        else:
            return {"Already Exists!": "Database Already Exists!"}
    except Exception as exception:
        return(None, exception)

def delete_database_function(database: str):
    """
    Deletes a database with the given name.

    Args:
        database (str): The name of the database to be deleted.

    Returns:
        dict or Exception: A dictionary with the keys "Ok" and "Database Deleted!" if the database is successfully deleted.
        If the database doesn't exist, a dictionary with the keys "Doesn't Exist!" and "Database doesn't Exist!" is returned.
        If an exception occurs during the deletion process, the exception object is returned.
    """
    try:
        engine = create_engine(URL)
        connection = engine.connect()
        if database_exists(URL+f"/{database}"):
            value = connection.execute(drop_database(URL+f"/{database}"))
            if value is None:
                return({"Ok": "Database Deleted!"})
        else:
            return({"Doesn't Exist!": "Database doesn't Exist!"})
    except Exception as exception:
        return(exception)

def create_tables(database: str,*table_names: str):
    """
    Creates tables in a given database.

    Args:
        database (str): The name of the database.
        *table_names (str): Variable length argument list of table names.

    Returns:
        str or dict: If the tables are created successfully, returns "Table already exists!" if the table already exists, or returns a dictionary with the error message "Database Does not exist!" if the database does not exist. If an exception occurs, returns the exception object.

    Raises:
        None
    """
    try:
        engine = create_engine(URL+f"/{database}", echo= True)
        inspector = inspect(engine)
        if database_exists(URL+f"/{database}"):
            for table_name in table_names:
                if table_name not in inspector.get_table_names():
                    table_name  = Table(
                    f"{table_name}", METADATA,
                    Column("Id", Integer, primary_key = True))
                    table_name.create(bind=engine)
                else:
                    return("Table already exists!")
        else:
            return({"Error!": "Database Does not exist!"})
    except Exception as exception:
        return exception

def delete_tables(database: str, *table_names: str):
    """
    Deletes specified tables from a given database.

    Args:
        database (str): The name of the database.
        *table_names (str): Variable number of table names to be deleted.

    Returns:
        str: Returns "Tables don't exist!" if any of the specified tables don't exist in the database.
             Returns "database doesn't exist!" if the specified database doesn't exist.
             Returns the exception if any other error occurs during the deletion process.
    """
    try:
        engine = create_engine(URL+f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL+f"/{database}"):
            for table in table_names:
                if table in inspector.get_table_names():
                    table = Table(f"{table}", MetaData(bind=engine), autoload=True, autoload_with=engine)
                    table.drop(bind = engine)
                else:
                    return("Tables don't exist!")
        else:
            return("database doesn't exist!")
    except Exception as exception:
        return exception

def insert_columns(database: str, table_name: str,  column_name: str, datatype: str, size: str, command: str):
    """
    Inserts columns into a table in a database.

    Parameters:
        - database (str): The name of the database.
        - table_name (str): The name of the table.
        - column_name (str): The name of the column to be inserted.
        - datatype (str): The datatype of the column.
        - size (str): The size of the column.
        - command (str): Additional command to be executed.

    Returns:
        - str: A success message if the columns were inserted successfully.
        - Exception: An exception object if an error occurred during the operation.
    """
    try:
        engine = create_engine(URL+f"/{database}")
        inpsector = inspect(engine)
        command = str(command)
        command.lower()
        if database_exists(URL+f"/{database}"):
            if table_name in inpsector.get_table_names():
                command = text(f"ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {command}")
                engine.execute(command)
                return("Successfully inserted columns!")
    except Exception as exception:
        return(exception)
        #         if len(order) > 0 and order == "first":
        #             command = text(f"ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {order}")
        #         else:
        #             return{"Error":"Invalid order!"}
        #         if len(column) > 0 and order == "after":
        #             command = text(f"ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {order} {column}")
        #     else:
        #         return {"Error":"Table does not exist!"}
        # else:
        #     return{"Error":"Database does not exist!"}

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
        engine = create_engine(URL+f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL+f"/{database}"):
            if table_name in inspector.get_table_names():
                command = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
                engine.execute(command)
            else:
                return{"Error": "Table does not exist!"}
        else:
            return{"Error": "Database does not exist!"}
    except Exception as exception:
        return exception

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
        engine = create_engine(URL+f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL+f"{database}"):
            if table_name in inspector.get_table_names():
                    command = f"ALTER TABLE {table_name} MODIFY {column_name} {command}"
                    engine.execute(command)
            else:
                return("table doesn't exist")
        else:
            return("Database doesn't exist")
    except Exception as exception:
        return exception

def inspect_columns(database: str, table, *column: str):
    """
    Inspects the columns of a database table.

    :param database: The name of the database.
    :param table: The name of the table to inspect.
    :param column: Variable length argument representing the columns to retrieve. 
                   If no columns are specified, all columns will be retrieved.

    :return: If the table exists in the database, returns a list of columns in the table. 
             If the table exists and columns are specified, returns the data from the specified columns. 
             If the table doesn't exist, returns the string "Table doesn't Exist!".
             If the database doesn't exist, returns the string "Database doesn't exist".
             If any exception occurs during execution, returns the exception object.
    """
    try:
        engine = create_engine(URL+ f"/{database}")
        inspector = inspect(engine)
        table = Table(f"{table}", MetaData(bind=engine), autoload=True, autoload_with=engine)
        if database_exists(URL+f"/{database}"):
            if table in inspector.get_table_names():
                return engine.execute(f"SHOW COLUMNS IN {table}").fetchall()
            elif table in inspector.get_table_names() and len(column) > 0:
                return engine.execute(f"SELECT * FROM {column}").fetchall()
            else:
                return("Table doesn't Exist!")
        else:
            return("Database doesn't exist")
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
        engine = create_engine(URL+ f"/{database}")
        inspector = inspect(engine)
        table = Table(f"{table_name}", MetaData(bind=engine), autoload=True, autoload_with=engine)
        if database_exists(URL+f"/{database}"):
            if table in inspector.get_table_names():
                engine.execute(f"SELECT * FROM {table} WHERE {filter_condition}").fetchall()
    except Exception as exception:
        return exception
