from sqlalchemy import create_engine, Table, Column, Integer, MetaData, inspect, text
from sqlalchemy_utils import create_database, database_exists, drop_database
import pandas as pd
import toml
import os
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import ProgrammingError
import sys

METADATA = MetaData()

def get_toml_credentials():
    """
    Read the TOML credentials file and return its contents.

    :return: A dictionary containing the contents of the TOML file.
    """
    home_directory = os.path.expanduser('~')
    if not os.path.exists(rf"{home_directory}/.database_credentials.toml") or os.path.getsize(rf"{home_directory}/.database_credentials.toml") == 0:
        create_credentials_file()

    with open(rf"{home_directory}/.database_credentials.toml", 'r') as f:
        toml_file = toml.load(f)
    return toml_file.get("credentials")


def create_credentials_file():
    """
    Creates a credentials file with the user's input for username, password, and host.

    Parameters:
        None

    Returns:
        credentials (dict): A dictionary containing the user's credentials
    """


    username = input("Enter your username. Default value is root: ") or "root"
    password = input("Enter your password: ")
    host = input("Enter your host. Default value is localhost: ") or "localhost"
    port = input("Enter your port. Default value is 3306: ") or "3306"
    connector = input("Enter your connector. Default value is mysqlconnector: ") or "mysql+mysqlconnector"
    file_name = ".database_credentials.toml"

    # Create the file
    home_directory = os.path.expanduser('~')
    file_path = os.path.join(home_directory, file_name)
    data = {
        "credentials": {
            "USER": username,
            "PASSWORD": password,
            "HOSTNAME": host,
            "PORT": port,
            "CONNECTOR": connector
        }
    }

    with open(file_path, "w") as file:
        sys.stdout.write("Creating credentials file in {file_path}...".format(file_path=file_path))
        toml.dump(data, file)

    with open(file_path, "r") as file:
        return toml.load(file)["credentials"]

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
        credentials = get_toml_credentials()
        URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
        engine = create_engine(URL)
        connection = engine.connect()
        if not database_exists(f"{URL}/{database}"):
            connection.execute(create_database(f"{URL}/{database}"))
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
        credentials = get_toml_credentials()
        URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
        with create_engine(URL).connect() as connection:
            if database_exists(f"{URL}/{database}"):
                connection.execute(drop_database(f"{URL}/{database}"))
                return {"Ok": "Database Deleted!"}
            else:
                return {"Doesn't Exist!": "Database doesn't Exist!"}
    except Exception as exception:
        return exception

def create_tables(database: str, *table_names: str, column_name: str = None):
    """
    Creates tables in a specified database.

    Parameters:
        database (str): The name of the database to create tables in.
        *table_names (str): Variable length argument list of table names to create.

    Returns:
        str or dict: If the tables are successfully created, returns 'Table already exists!' if the table already exists in the database. If the database does not exist, returns a dictionary with the key 'Error!' and the value 'Database does not exist!'. If an exception occurs during the table creation process, returns the exception object.
    """
    try:
        credentials = get_toml_credentials()
        USER = credentials.get("USER")
        PASSWORD = credentials.get("PASSWORD")
        HOSTNAME = credentials.get("HOSTNAME")
        connector = credentials.get("CONNECTOR")
        URL = f"{connector}://{USER}:{PASSWORD}@{HOSTNAME}/{database}"
        engine = create_engine(URL, echo=True)
        inspector = inspect(engine)

        if database_exists(URL):
            sys.stdout.write(column_name)
            for table_name in table_names:
                if table_name not in inspector.get_table_names():
                    if column_name:
                        table = Table(table_name, METADATA, Column('Id', Integer, primary_key=True), Column(column_name, Integer))
                        table.create(bind=engine)
                    else:
                        table = Table(table_name, METADATA, Column('Id', Integer, primary_key=True))
                        table.create(bind=engine)
                else:
                    return 'Table already exists!'
        else:
            return {'Error!': 'Database does not exist!'}
    except Exception as e:
        return e

def delete_tables(database: str, *table_names: str):
    """
    Deletes specified tables from a given database.

    Args:
        database (str): The name of the database.
        *table_names (str): The names of the tables to be deleted.

    Returns:
        str or Exception: Returns "database doesn't exist!" if the database doesn't exist,
            "Tables don't exist!" if the specified tables don't exist, or an Exception object
            if any other error occurs.
    """
    try:
        credentials = get_toml_credentials()
        URL = f"{credentials.get('CONNECTOR')}://{credentials.get('USER')}:{credentials.get('PASSWORD')}@{credentials.get('HOSTNAME')}"
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
    """
    Insert columns into a database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table to insert columns into.
        column_name (str): The name of the column to be inserted.
        datatype (str): The datatype of the column.
        size (str): The size of the column.
        command (str): The command to be executed after inserting the column.

    Returns:
        str: A success message if the columns are inserted successfully, otherwise an exception message.

    Raises:
        Exception: If an error occurs during the insertion process.
    """
    try:
        credentials = get_toml_credentials()
        USER = credentials.get("USER")
        PASSWORD = credentials.get("PASSWORD")
        HOSTNAME = credentials.get("HOSTNAME")
        connector = credentials.get("CONNECTOR")
        URL = f"{connector}://{USER}:{PASSWORD}@{HOSTNAME}/{database}"

        engine = create_engine(URL)
        inspector = inspect(engine)

        if database_exists(URL) and table_name in inspector.get_table_names():
            command = text(f'ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {command}')
            engine.execute(command)
            return 'Successfully inserted columns!'
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
        dict: An empty dictionary if the column deletion is successful.
        If any SQLAlchemy errors occur during the deletion, the exception is returned.
        If the database or table does not exist, an error message is returned.
    """
    try:
        credentials = get_toml_credentials()
        URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}/{database}"

        engine = create_engine(URL)
        inspector = inspect(engine)

        if not inspector.has_database(database):
            return {"Error": "Database does not exist!"}

        if not inspector.has_table(table_name):
            return {"Error": "Table does not exist!"}

        command = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
        engine.execute(command)
    except SQLAlchemyError as exception:
        return exception

    return {}

def modify_column(database: str, table_name: str, column_name: str, command: str):
    """
    Modifies a column in a database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column.
        command (str): The modification command to execute.

    Returns:
        str: If the modification is successful, returns an empty string. Otherwise, returns an error message.
    """
    try:
        credentials = get_toml_credentials()
        URL = f"{credentials.get('CONNECTOR')}://{credentials.get('USER')}:{credentials.get('PASSWORD')}@{credentials.get('HOSTNAME')}"

        with create_engine(URL + f"/{database}").connect() as connection:
            inspector = inspect(connection)
            if database_exists(URL + f"/{database}"):
                if table_name in inspector.get_table_names():
                    command = f"ALTER TABLE {table_name} MODIFY {column_name} {command}"
                    connection.execute(command)
                else:
                    return "table doesn't exist"
            else:
                return "Database doesn't exist"
    except Exception as exception:
        return str(exception)


def inspect_columns(database: str, table: str, *column: str):
    """
    Get information about columns in a database table.

    Args:
        database (str): The name of the database.
        table (str): The name of the table.
        *column (str): Variable length argument for column names.

    Returns:
        list: A list of column information if no column names are specified, or a list of values for the specified columns.
        str: "Table doesn't Exist!" if the table doesn't exist.
        Exception: An exception object if an error occurs.
    """

    try:
        credentials = get_toml_credentials()
        URL = f"{credentials.get('CONNECTOR')}://{credentials.get('USER')}:{credentials.get('PASSWORD')}@{credentials.get('HOSTNAME')}"
        engine = create_engine(URL + f"/{database}")
        inspector = inspect(engine)

        if inspector.has_table(table):
            if not column:
                return inspector.get_columns(table)
            else:
                return engine.execute(f"SELECT {', '.join(column)} FROM {table}").fetchall()
        else:
            return "Table doesn't Exist!"
    except Exception as exception:
        return exception

def query(database: str, table_name: str, filter_condition: str):
    """
    Executes a query on the specified database table using the provided filter condition.

    Args:
        database (str): The name of the database to query.
        table_name (str): The name of the table to query.
        filter_condition (str): The condition to filter the query results.

    Returns:
        list: A list of rows that match the filter condition.
        Exception: If any error occurs during the query execution.
    """
    try:
        credentials = get_toml_credentials()
        URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
        with create_engine(f"{URL}/{database}").connect() as connection:
            inspector = inspect(connection)
            if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
                return connection.execute(f"SELECT * FROM {table_name} WHERE {filter_condition}").fetchall()
    except Exception as exception:
        return exception



def check_for_duplicates(database: str, table_name: str, column_name: str):
    """
    Check for duplicates in a specific column of a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column to check for duplicates.

    Returns:
        list: A list of rows representing the duplicate values found in the column.
    """

    credentials = get_toml_credentials()
    URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
    with create_engine(f"{URL}/{database}").connect() as connection:
        inspector = inspect(connection)
        if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
            query = f"SELECT {column_name} FROM {table_name} GROUP BY {column_name} HAVING COUNT({column_name}) > 1"
            return connection.execute(query).fetchall()



def delete_duplicates(dataframe: pd.DataFrame):
    """
    Delete duplicates from the given dataframe.

    Args:
        dataframe (pd.DataFrame): The dataframe to remove duplicates from.

    Returns:
        pd.DataFrame: The dataframe with duplicates removed.
    """
    if type(dataframe) != pd.DataFrame:
        sys.stdout.write("Not a pandas dataframe, trying to convert to pandas dataframe\n")
        dataframe = pd.DataFrame(pd.read_csv(dataframe, encoding="cp1252"))
        return dataframe.drop_duplicates()


def get_data_from_database(database: str = None, table_name: str = None):
    """
    Retrieves data from a specified database table.

    Args:
        database (str, optional): The name of the database. Defaults to None.
        table_name (str, optional): The name of the table. Defaults to None.

    Returns:
        list: A list of tuples containing the retrieved data.
        str: An error message if there is an exception.

    Raises:
        ProgrammingError: If there is an exception while executing the SQL query.
    """
    if not database or not table_name:
        return "Please provide both database and table name as arguments to the function. \nExample: get_data_from_database(database='food_db', table_name='food_recipes')"

    try:
        credentials = get_toml_credentials()
        url = generate_database_url(credentials, database=database)
        with create_engine(url).connect() as connection:
            if connection:
                databases = [row[0] for row in connection.execute(text("SHOW DATABASES")).fetchall()]
                if database in databases:
                    tables = [ row[0] for row in connection.execute(text("SHOW TABLES IN " + database)).fetchall()]
                    if table_name in tables:
                        return connection.execute(text(f"SELECT * FROM {database}.{table_name}")).fetchall()
                    else:
                        return f"""The table '{table_name}' does not exist. Trying with capitalized table name. {connection.execute(text(f"SELECT * FROM {database}.{table_name.upper()}")).fetchall()}"""
                else:
                    return f"The database '{database}' does not exist. Try with capitalized database name. {databases}"

    except ProgrammingError as exception:
        return exception

def generate_database_url(credentials: dict, database: str):
    """
    Generate a database URL using the given credentials and database name.

    Args:
        credentials (dict): A dictionary containing the connection credentials.
            It should have the following keys:
                - CONNECTOR (str): The type of database connector.
                - USER (str): The username for the database connection.
                - PASSWORD (str): The password for the database connection.
                - HOSTNAME (str): The hostname for the database connection.
        database (str): The name of the database.

    Returns:
        str: The generated database URL.

    Example:
        >>> credentials = {
        ...     'CONNECTOR': 'mysql',
        ...     'USER': 'root',
        ...     'PASSWORD': 'password',
        ...     'HOSTNAME': 'localhost'
        ... }
        >>> generate_database_url(credentials, 'mydatabase')
        'mysql://root:password@localhost/mydatabase'
    """
    return f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}/{database}"


def insert_dataframe(database: str, table_name: str, dataframe: pd.DataFrame):
    """
    Inserts a DataFrame into a specified database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        dataframe (pd.DataFrame): The DataFrame to be inserted.

    Returns:
        str or ProgrammingError: A success message if the DataFrame is inserted successfully,
            or a ProgrammingError if an exception occurs.
    """
    dataframe = delete_duplicates(dataframe)
    print("Dataframe created successfully!")
    url = generate_database_url(get_toml_credentials(), database)
    print("Database URL generated successfully!")
    try:
        if database_exists(url):
            with create_engine(url).connect() as connection:
                if not database_exists(url):
                    create_database_function(database)
                if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                    dataframe.to_sql(name=table_name, con=create_engine(url), if_exists='replace', index=False)
                return "Dataframe inserted successfully!"
        else:
            create_database_function(database)
            return insert_dataframe(database=database, table_name=table_name, dataframe=dataframe)
    except ProgrammingError as e:
        return e
if __name__ == '__main__':
    print(insert_dataframe(database="isro", table_name="isro_mission_launches", dataframe="ISRO mission launches.csv"))
    # print(get_data_from_database(database="isro", table_name="isro_mission_launches"))