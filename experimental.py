from sqlalchemy import Column, Integer, MetaData, Table, create_engine, inspect, text, update, delete, select,insert
from sqlalchemy_utils import create_database, database_exists, drop_database

USER = "root"
PASSWORD = "MYmac123"
HOSTNAME = "localhost"
METADATA = MetaData()
URL = f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}"


def show_databases(url: str):
    """
    Retrieves a list of all the databases available in the specified URL.

    Parameters:
        url (str): The URL of the database.

    Returns:
        list: A list of strings, where each string represents the name of a database.

    Raises:
        Exception: If there is an error retrieving the database list.
    """
    try:
        engine = create_engine(url)
        inspector = inspect(engine)
        return inspector.get_schema_names()
    except Exception as exception:
        return exception

# DDL
def create_database_function(database: str):
    """
    Create a database function.

    Parameters:
        database (str): The name of the database to be created.

    Returns:
        dict or tuple: If the database is created successfully, returns a dictionary
        with the key "Ok" and value "Database Created!". If the database already
        exists, returns a dictionary with the key "Already Exists!" and value
        "Database Already Exists!". If an exception occurs, returns a tuple with
        None as the first element and the exception as the second element.
    """
    try:
        engine = create_engine(URL)
        connection = engine.connect()
        if not database_exists(URL + f"/{database}"):
            connection.execute(create_database(URL + f"/{database}"))
            return {"Ok": "Database Created!"}
        else:
            return {"Already Exists!": "Database Already Exists!"}
    except Exception as exception:
        return (None, exception)


def delete_database_function(database: str):
    """
    Delete a database.

    Args:
        database (str): The name of the database to be deleted.

    Returns:
        dict or Exception: If the database is successfully deleted, the function returns a dictionary with the key "Ok" and the value "Database Deleted!". If the database doesn't exist, the function returns a dictionary with the key "Doesn't Exist!" and the value "Database doesn't Exist!". If an exception occurs during the deletion process, the function returns the exception object.
    """
    try:
        engine = create_engine(URL)
        connection = engine.connect()
        if database_exists(URL + f"/{database}"):
            value = connection.execute(drop_database(URL + f"/{database}"))
            if value is None:
                return {"Ok": "Database Deleted!"}
        else:
            return {"Doesn't Exist!": "Database doesn't Exist!"}
    except Exception as exception:
        return exception


def create_tables(database: str, *table_names: str):
    """
    Create tables in the specified database.

    Args:
        database (str): The name of the database to create tables in.
        *table_names (str): Variable length argument list of table names.

    Returns:
        str or dict: If the tables are created successfully, returns "Table already exists!" 
        if the table already exists. If the database does not exist, returns a dictionary 
        with the key "Error!" and the value "Database Does not exist!". If an exception occurs, 
        returns the exception object.
    """
    try:
        engine = create_engine(URL + f"/{database}", echo=True)
        inspector = inspect(engine)
        if database_exists(URL + f"/{database}"):
            for table_name in table_names:
                if table_name not in inspector.get_table_names():
                    update(table_name, )
                    table_name = Table(
                        f"{table_name}",
                        METADATA,
                        Column("Id", Integer, primary_key=True),
                    )
                    table_name.create(bind=engine)
                else:
                    return "Table already exists!"
        else:
            return {"Error!": "Database Does not exist!"}
    except Exception as exception:
        return exception


def delete_tables(database: str, *table_names: str):
    """
    Deletes the specified tables from the given database.

    Args:
        database (str): The name of the database.
        *table_names (str): The names of the tables to be deleted.

    Returns:
        str: A message indicating the success or failure of the operation. 

    Raises:
        Exception: If an error occurs during the deletion process.
    """
    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL + f"/{database}"):
            for table in table_names:
                if table in inspector.get_table_names():
                    table = Table(
                        f"{table}",
                        MetaData(bind=engine),
                        autoload=True,
                        autoload_with=engine,
                    )
                    table.drop(bind=engine)
                else:
                    return "Tables don't exist!"
        else:
            return "database doesn't exist!"
    except Exception as exception:
        return exception


def insert_columns(
    database: str,
    table_name: str,
    column_name: str,
    datatype: str,
    size: str,
):
    """
    Inserts a new column into a specified table in the given database.

    Args:
        database (str): The name of the database to insert the column into.
        table_name (str): The name of the table to insert the column into.
        column_name (str): The name of the new column.
        datatype (str): The data type of the new column.
        size (str): The size of the new column.

    Returns:
        None

    Raises:
        Exception: If there is an error inserting the column.
    """

    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL + f"/{database}") and table_name in inspector.get_table_names():
            sqlalchemy_command = insert(table=table_name,)
            command = text(f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} {datatype}({size})")
            engine.execute(command)
            print("Successfully inserted columns!")
        else:
            print("Error")
    except Exception as exception:
        print(exception)

    except Exception as exception:
        return exception

def delete_columns(database: str, table_name: str, column_name: str):
    """
    Deletes a column from a specified table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column to be deleted.

    Returns:
        dict or Exception: If the column is successfully deleted, returns an empty dictionary.
                          If the table does not exist, returns a dictionary with the error message "Table does not exist!".
                          If the database does not exist, returns a dictionary with the error message "Database does not exist!".
                          If an exception occurs during the deletion process, returns the exception object.
    """

    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL + f"/{database}"):
            if table_name in inspector.get_table_names():
                command = text(f"ALTER TABLE `{table_name}` DROP COLUMN {column_name}")
                engine.execute(command)
            else:
                return {"Error": "Table does not exist!"}
        else:
            return {"Error": "Database does not exist!"}
    except Exception as exception:
        return exception

def modify_column(database: str, table_name: str, column_name: str, command: str):
    """
    Modify a column in a database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table where the column exists.
        column_name (str): The name of the column to be modified.
        command (str): The modification command to be executed.

    Returns:
        str: If successful, returns an empty string. If the table doesn't exist, 
        returns "table doesn't exist". If the database doesn't exist, returns 
        "Database doesn't exist". If an exception occurs, returns the exception object.
    """
    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL + f"{database}"):
            if table_name in inspector.get_table_names():
                command = text(f"ALTER TABLE `{table_name}` MODIFY {column_name} {command}")
                engine.execute(command)
            else:
                return "table doesn't exist"
        else:
            return "Database doesn't exist"
    except Exception as exception:
        return exception

def get_table_names(database: str):
    """
    Retrieves the names of all tables in the specified database.

    Args:
        database (str): The name of the database.

    Returns:
        list: A list of table names in the database.

    Raises:
        Exception: If an error occurs while retrieving the table names.
    """

    try:
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)
        return engine.execute(inspector.get_table_names()).fetchall()
    except Exception as exception:
        return exception

def update_table(database: str, table_name:Table, *column_values: str):
    """
    Updates a table in a database with the provided column values.

    Args:
        database (str): The name of the database.
        table_name (Table): The name of the table to update.
        *column_values (str): The values to update the table with.

    Returns:
        Exception: Returns an exception if an error occurs during the update.
    """
    try:
        engine = create_engine(URL+f"/{database}")
        inspector = inspect(engine)
        if database_exists(URL+f'/{database}') and table_name in inspector.get_table_names():
            columns = engine.execute(f"SHOW COLUMNS IN {table_name}").fetchall()
            for value in column_values:
                for column in columns:
                    command = f"UPDATE {table_name}({column}) SET VALUES({value})"
            engine.execute(command)
            print("tables updated")
        else:
            print("table doesn't exist")
    except Exception as exception:
        return exception

if __name__ == "__main__":
    pass
