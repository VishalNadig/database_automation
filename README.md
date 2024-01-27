# database_automation
Python code with sqlalchemy for automating querying the database.


# DOCUMENTATION

# GENERAL INFORMATION
1. You will need the kaggle API keys to access the kaggle datasets.
2. You will need the database credentials file. You can create one if it does not exist!

## get_yaml_credentials:
    """
    Read the YAML credentials file and return its contents.

    Returns: A dictionary containing the contents of the YAML file.
    """

## create_credentials_file:
    """
    Creates a credentials file with the user's input for username, password, and host.


    Returns:
        credentials (dict): A dictionary containing the user's credentials
    """

## create_database_function:
    """
    Create a database function.

    Args:
        database (str): The name of the database to be created.

    Returns:
        dict: A dictionary containing the status of the database creation. If the database is created successfully, the dictionary will have the key "Ok" and the value "Database Created!". If the database already exists, the dictionary will have the key "Already Exists!" and the value "Database Already Exists!".
        tuple: If an exception occurs during the creation of the database, a tuple with None as the first element and the exception as the second element will be returned.
    """

## delete_database_function:
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

## create_tables:
    """
    Creates tables in a specified database.

    Parameters:
        database (str): The name of the database to create tables in.
        *table_names (str): Variable length argument list of table names to create.

    Returns:
        or dict: If the tables are successfully created, returns 'Table already exists!' if the table already exists in the database. If the database does not exist, returns a dictionary with the key 'Error!' and the value 'Database does not exist!'. If an exception occurs during the table creation process, returns the exception object.
    """

## delete_tables:
    """
    Deletes specified tables from a given database.

    Args:
        database (str): The name of the database.
        *table_names (str): The names of the tables to be deleted.

    Returns:
        or Exception: Returns "database doesn't exist!" if the database doesn't exist,
            "Tables don't exist!" if the specified tables don't exist, or an Exception object
            if any other error occurs.
    """
    

## insert_columns:
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
       : A success message if the columns are inserted successfully, otherwise an exception message.

    Raises:
        Exception: If an error occurs during the insertion process.
    """

## delete_columns:
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
    

## modify_column:
    """
    Modifies a column in a database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column.
        command (str): The modification command to execute.

    Returns:
       : If the modification is successful, returns an emptying. Otherwise, returns an error message.
    """

## inspect_columns:
    """
    Get information about columns in a database table.

    Args:
        database (str): The name of the database.
        table (str): The name of the table.
        *column (str): Variable length argument for column names.

    Returns:
        list: A list of column information if no column names are specified, or a list of values for the specified columns.
       : "Table doesn't Exist!" if the table doesn't exist.
        Exception: An exception object if an error occurs.
    """

## query:
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
    
## check_for_duplicates:
    """
    Check for duplicates in a specific column of a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        column_name (str): The name of the column to check for duplicates.

    Returns:
        list: A list of rows representing the duplicate values found in the column.
    """

  ## delete_duplicates:
    """
    Delete duplicates from the given dataframe.

    Args:
        dataframe (pd.DataFrame): The dataframe to remove duplicates from.

    Returns:
        pd.DataFrame: The dataframe with duplicates removed.
    """

## get_data_from_database:
    """
    Retrieves data from a specified database table.

    Args:
        database (str, optional): The name of the database. Defaults to None.
        table_name (str, optional): The name of the table. Defaults to None.

    Returns:
        list: A list of tuples containing the retrieved data.
       : An error message if there is an exception.

    Raises:
        ProgrammingError: If there is an exception while executing the SQL query.
    """

## generate_database_url:
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
       : The generated database URL.

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

## insert_dataframe:
    """
    Inserts a DataFrame into a specified database table.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        dataframe (pd.DataFrame): The DataFrame to be inserted.

    Returns:
        or ProgrammingError: A success message if the DataFrame is inserted successfully,
            or a ProgrammingError if an exception occurs.
    """

## add_new_data_to_table:
    """
    Adds new data to a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.

    Returns:
        None
    """
  
## add_pk:
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

## delete_pk:
    """
    Delete constraints from a table in a given database.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table.
        constraint_name (str): The name of the constraint.

    Returns:
        None
    """
    
## search_kaggle_datasets:
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
    
## download_kaggle_dataset:
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
    
## upload_dataset_to_database:
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
  
## download_dataset_from_database:
    """
    Downloads a dataset from a database table.

    Parameters:
        database (str): The name of the database.
        table_name (str): The name of the table in the database.
        download_path (str): The path to download the dataset to.

    Returns:
        None
    """
