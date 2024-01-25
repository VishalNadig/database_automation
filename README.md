# database_automation
Python code with sqlalchemy for automating querying the database.


# DOCUMENTATION
def get_yaml_credentials():
    """
    Read the YAML credentials file and return its contents.

    Returns: A dictionary containing the contents of the YAML file.
    """

def create_credentials_file():
    """
    Creates a credentials file with the user's input for username, password, and host.


    Returns:
        credentials (dict): A dictionary containing the user's credentials
    """

def create_database_function(database: str):
    """
    Create a database function.

    Args:
        database (str): The name of the database to be created.

    Returns:
        dict: A dictionary containing the status of the database creation. If the database is created successfully, the dictionary will have the key "Ok" and the value "Database Created!". If the database already exists, the dictionary will have the key "Already Exists!" and the value "Database Already Exists!".
        tuple: If an exception occurs during the creation of the database, a tuple with None as the first element and the exception as the second element will be returned.
    """

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

def create_tables(database: str, *table_names: str, column_name: str = None):
    """
    Creates tables in a specified database.

    Parameters:
        database (str): The name of the database to create tables in.
        *table_names (str): Variable length argument list of table names to create.

    Returns:
        str or dict: If the tables are successfully created, returns 'Table already exists!' if the table already exists in the database. If the database does not exist, returns a dictionary with the key 'Error!' and the value 'Database does not exist!'. If an exception occurs during the table creation process, returns the exception object.
    """

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

  def delete_duplicates(dataframe: pd.DataFrame):
    """
    Delete duplicates from the given dataframe.

    Args:
        dataframe (pd.DataFrame): The dataframe to remove duplicates from.

    Returns:
        pd.DataFrame: The dataframe with duplicates removed.
    """

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

def add_new_data_to_table(database: str = None, table_name: str = None, dataframe: pd.DataFrame = None):
    """
    Adds new data to a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.

    Returns:
        None
    """
  
def add_pk(database: str, table_name: str, constraint_name: str, column_name: str, delete_constraint: bool):
    """
    Add constraints to a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        constraint_name (str): The name of the constraint.
        column_name (str): The name of the column to be constrained.

    Returns:
        None
    """

def delete_pk(database: str, table_name: str):
    """
    Delete constraints from a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        constraint_name (str): The name of the constraint.

    Returns:
        None
    """
    
def search_kaggle_datasets(dataset: str = None, user: str = None):
    """
    Searches for a Kaggle dataset and returns the dataset information in a dictionary.

    Parameters:
        dataset (str): The name of the dataset to search for.
        user (str): The name of the user to search for.

    Returns:
        dict: A dictionary containing the dataset information. The keys are the indices of the datasets, and the values are the dataset objects.

    Raises:
        None

    Example:
        search_kaggle_datasets("video game")
    """
    
def download_kaggle_dataset(dataset: str, dataset_path: str = None):
    """
    Download a Kaggle dataset.

    Args:
        dataset (str): The name of the Kaggle dataset.
        path (str, optional): The path to save the downloaded files. Defaults to None.

    Returns:
        None

    Raises:
        None
    """
    
def upload_dataset_to_database(database: str = None, table_name: str = None, dataset: str = None, user: str = None, dataset_path: str = None):
    """
    Uploads a dataset to a database table.

    Parameters:
        database (str): The name of the database.
        table_name (str): The name of the table in the database.
        dataset (str): The name of the dataset to upload.
        user (str): The name of the user who owns the dataset.
        dataset_path (str): The path to the dataset on the local machine.

    Returns:
        None
    """
  
def download_dataset_from_database(database: str, table_name: str, download_path: str):
    """
    Downloads a dataset from a database table.

    Parameters:
        database (str): The name of the database.
        table_name (str): The name of the table in the database.
        download_path (str): The path to download the dataset to.

    Returns:
        None
    """
