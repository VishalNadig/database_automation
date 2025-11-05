import pandas as pd
import yaml
import os
import boto3
import argparse
import sys
import re
from configparser import ConfigParser
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, Text, Date, DateTime, CHAR, Boolean, inspect, text
from sqlalchemy_utils import create_database, database_exists, drop_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import ProgrammingError
from cryptography.fernet import Fernet
from typing import Optional, Tuple
from log_handler import Logger


class DatabaseHandler():
    """Database Handler class that contains all the functions to manipulate data and interact with databases using sqlalchemy"""
    def __repr__(self):
        return "DatabaseHandler(username='insert_your_name')"


    def __str__(self):
        return "Database Handler class that contains all the functions to manipulate data and interact with databases using sqlalchemy"


    def __init__(self, username: str = None):
        self.METADATA = MetaData()
        self.sqlalchemy_type_map = {
            "VARCHAR": lambda size: String(size),
            "CHAR": lambda size: CHAR(size),
            "INT": lambda: Integer(),
            "FLOAT": lambda: Float(),
            "TEXT": lambda: Text(),
            "DATE": lambda: Date(),
            "DATETIME": lambda: DateTime(),
            "BOOLEAN": lambda: Boolean()
        }
        self.database_connector = {}
        self.HOME = os.path.expanduser('~')
        self.USERNAME = username.lower().replace(" ", "_") or input("Enter your username: ").replace(" ", "_").lower()
        self.logger = Logger(name="database_handler", filename=os.path.join(self.HOME, "database_handler.log"))
        self.logger.log("Logger initialized for database_handler")
        if os.path.exists(os.path.join(self.HOME, ".aws/credentials")):
            self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
            self.SESSION = boto3.Session(profile_name=self.USERNAME)
            self.CONFIG = self.SESSION.get_credentials()
        elif os.path.exists(os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml")):
            self.CONFIG_PATH = os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml")
            with open(os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml"), "r") as file:
                self.CONFIG = yaml.safe_load(file)
        else:
            self.CONFIG_PATH = None
        if self.CONFIG_PATH is None:
            self.CONFIG_PATH, self.CONFIG = self.create_credentials_file()
        self.CREDENTIALS = self.get_credentials()


    def get_args(self) -> argparse.Namespace:
        """
        Read the command line arguments and return them.

        Args:
            None

        Returns:
            argparse.Namespace: Namespace of all the flags used in the CLI.
        """
        self.args = argparse.ArgumentParser(description='Fitbit API')
        self.args.add_argument('-u', '--username', help='Username for Fitbit API')
        self.args.add_argument('-p', '--password', help='Password for Fitbit API')
        self.args.add_argument('-c', '--config_path', help='Path to the YAML credentials file')
        self.args.add_argument('-P', '--port', help='Port to connect to the local database')
        self.args.add_argument('-C', '--connector', help = "Type of database to connect to. List of connectors: \n[1] mysql+mysqlconnector\n[2] postgresql+psycopg2\n[3] mssql+pyodbc\n[4] sqlite\n")
        self.args.add_argument("-H", "--hostname", help = "Hostname to connect to the local database")
        self.args.add_argument('-d', '--database_type', help="Type of database, either local or aws")
        self.args.add_argument('-a', "--aws_access_key", help = "AWS Access Key")
        self.args.add_argument('-s', "--aws_secret_key", help = "AWS Secret Key")
        self.args.add_argument('-r', "--aws_region", help = "Region name of the AWS server")
        return self.args.parse_args()


    def get_credentials(self) -> Tuple[str, dict]:
        """
        Read the YAML credentials file and return its contents.

        :return: A dictionary containing the contents of the YAML file.
        """
        if self.CONFIG_PATH is None:
            self.create_credentials_file()
        elif ".aws" in self.CONFIG_PATH:
            config = ConfigParser()
            config.read(self.CONFIG_PATH)
            self.logger.log(f"ConfigParser file loaded from {self.CONFIG_PATH}")
            return self.CONFIG_PATH, config
        else:
            with open(self.CONFIG_PATH, "r") as file:
                yaml_file = yaml.safe_load(file)
                self.logger.log(f"YAML file loaded from {self.CONFIG_PATH}")
                return self.CONFIG_PATH, yaml_file


    def local_database_file_handler(self, args: argparse.Namespace) -> Tuple[str, dict]:
        self.CONFIG_PATH = os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml")
        username = args.username or input("Enter your database username. Default value is root: ") or "root"
        password = args.password or input("Enter your database password: ")
        while not password:
            password = input("Password cannot be empty. Enter your password: ")
        host = args.hostname or input("Enter your host URL for the database. Default value is localhost: ") or "localhost"
        port = args.port or input("Enter your port value for your database. Default value is 3306: ") or "3306"
        connector = args.connector or input("Enter your connector. List of connectors: \n[1] mysql+mysqlconnector\n[2] postgresql+psycopg2\n[3] mssql+pyodbc\n[4] sqlite\nDefault value is mysqlconnector: ") or "mysql+mysqlconnector"
        match connector:
            case "1":
                connector = "mysql"
            case "2":
                connector = "postgresql"
            case "3":
                connector = "mssql+pyodbc"
            case "4":
                connector = "sqlite"
            case _:
                connector = "mysql+mysqlconnector"
        self.CONFIG = {self.USERNAME.replace(" ", "_").lower(): {
            "user": username,
            "password": password,
            "hostname": host,
            "port": port,
            "connector": connector,
        }}
        with open(self.CONFIG_PATH, 'w') as file:
            yaml.safe_dump(self.CONFIG, file)
        self.logger.log(f"Credentials file created in {self.CONFIG_PATH}")
        return (self.CONFIG_PATH, self.CONFIG)


    def aws_credential_file_handler(self, args) -> Tuple[str, dict]:
        self.aws_access_key_id = args.aws_access_key or input("Enter your AWS access key ID: ")
        self.aws_secret_access_key = args.aws_secret_key or input("Enter your AWS secret access key: ")
        self.region_name = args.aws_region or input("Enter the region name. Defaults to us-east-1: ") or "us-east-1"
        if not os.path.exists(os.path.join(self.HOME, ".aws")):
            os.makedirs(os.path.join(self.HOME, ".aws"), exist_ok=True)
            self.logger.log(f"Created directory {os.path.join(self.HOME, '.aws')}")
        self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
        with open(self.CONFIG_PATH, 'w') as file:
            self.CONFIG[self.USERNAME] = {
                                    "aws_access_key_id": self.aws_access_key_id,
                                    "aws_secret_access_key": self.aws_secret_access_key,
                                    "region": self.region_name
                                }
            self.CONFIG.write(file)
        self.logger.log(f"Credentials file created in {self.CONFIG_PATH}")
        return (self.CONFIG_PATH, self.CONFIG)


    def create_credentials_file(self) -> Tuple[str, dict]:
        """
        Creates a credentials file with the user's input for username, password, and host.

        Parameters:
            None

        Returns:
            None
        """
        args = self.get_args()
        self.CONFIG = ConfigParser()
        if not args.database_type:
            self.CHOICE = input("Do you want to connect to an AWS DynamoDB or use a local database. Choosing 'n' will use a local database instead (y/n)?: ")
            while self.CHOICE.lower() not in ['y', 'n']:
                self.CHOICE = input("Do you want to connect to an AWS DynamoDB or use a local database. Choosing 'n' will use a local database instead (y/n)?: ")
            if self.CHOICE == 'y':
                return self.aws_credential_file_handler(args=args)
            else:
                self.warning_choice = input("Warning! You are using a local database and hence won't get cross device functionality and device sharing. Are you ok to continue? (y/n): ")
                while self.warning_choice.lower() not in ["y", "n"]:
                    self.warning_choice = input("Warning! You are using a local database and hence won't get cross device functionalit. Are you ok to continue? (y/n): ")
                if self.warning_choice == "n":
                    return "Not creating the database. Exiting..."
                return self.local_database_file_handler(args=args)
        elif args.database_type == "aws":
            return self.aws_credential_file_handler(args=args)
        else:
            return self.local_database_file_handler(args=args)


    def encrypt_data(self) -> dict:
        """Encrypt the sensitive credentials.

        Args:
            key (str, optional): The fernet key generated once at the start. Defaults to CONFIG['database_creds']['DATABASE_1']['key'].
            password (str, optional): The password to encrypt. Defaults to "".

        Returns:
            dict: Encrypted credentials dictionary 200 if successful. 404 If the password is wrong.
        """
        key = self.CONFIG.get("key", "key")
        password = self.CONFIG.get("password", "password")
        fernet = Fernet(key)
        return {"password": fernet.encrypt(password.encode())}


    def decrypt_data(self) -> dict:
        """Decrypt the sensitive credentials to use for API call authentication.

        Args:
            key (str, optional): The fernet key generated once at the start. Defaults to CONFIG['database_creds']['DATABASE_1']['key'].
            password (str, optional): The password to decrypt. Defaults to "".

        Returns:
            dict: Decrypted credentials dictionary 200 if successful. 404 If the password is wrong.
        """
        key = self.CONFIG.get("key", "key")
        password = self.CONFIG.get("password", "password")
        fernet = Fernet(key)
        return {"password": fernet.decrypt(bytes(password, "utf-8")).decode()}


    def get_database_connection(self, URL: str) -> None:
        """Get the connection to the database. The dictionary is used to ensure only one connection to the database exists, across the program without creating duplicate connections.

        Args:
            URL (str): The connection URL for the database.

        Returns:
            None.
        """
        engine = create_engine(URL)
        connection = engine.connect()
        if connection not in self.database_connector.values():
            self.database_connector['connection'] = connection
            return self.database_connector.get('connection')
        else:
            return self.database_connector.get('connection')


    def create_database_function(self, database: str) -> bool:
        """
        Create a database function.

        Args:
            database (str): The name of the database to be created.

        Returns:
            dict: A dictionary containing the status of the database creation. If the database is created successfully, the dictionary will have the key "Ok" and the value "Database Created!". If the database already exists, the dictionary will have the key "Already Exists!" and the value "Database Already Exists!".
            tuple: If an exception occurs during the creation of the database, a tuple with None as the first element and the exception as the second element will be returned.
        """
        try:

            URL = self.generate_database_url(credentials=self.CREDENTIALS, database=database)
            connection = self.get_database_connection(URL=URL)
            if not database_exists(f"{URL}/{database}"):
                connection.execute(create_database(f"{URL}/{database}"))
                self.logger.log(f"Database '{database}' created successfully.")
                return True
            self.logger.log(f"Database '{database}' already exists.")
            return False
        except Exception as exception:
            self.logger.log(f"An error occurred while creating the database: {exception}", level='exception')
            return (False, exception)


    def delete_database_function(self,database: str) -> bool:
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

            URL = self.generate_database_url(credentials=self.CREDENTIALS, database=database)
            with create_engine(URL).connect() as connection:
                if database_exists(f"{URL}/{database}"):
                    connection.execute(drop_database(f"{URL}/{database}"))
                    self.logger.log(f"Database '{database}' deleted successfully.")
                    return True
                else:
                    self.logger.log(f"Database '{database}' does not exist.", level='error')
                    return False
        except Exception as exception:
            self.logger.log(f"An error occurred while deleting the database: {exception}", level='exception')
            return False


    def parse_column_definition(self, col_name, raw_def) -> Column:
        """Parses a single column's raw SQL-style string and returns a SQLAlchemy Column object."""
        # Match SQL type like VARCHAR(50), INT, etc.
        type_match = re.search(r'(\w+)(\((\d+)\))?', raw_def)
        base_type = type_match.group(1).upper()
        size = type_match.group(3)

        if base_type not in self.sqlalchemy_type_map:
            raise ValueError(f"Unsupported type: {base_type}")

        col_type = (
            self.sqlalchemy_type_map[base_type](int(size)) if size else self.sqlalchemy_type_map[base_type]()
        )

        kwargs = {}

        if "PRIMARY KEY" in raw_def.upper():
            kwargs["primary_key"] = True
        if "NOT NULL" in raw_def.upper():
            kwargs["nullable"] = False

        default_match = re.search(r"DEFAULT\s+('[^']+'|\d+|CURRENT_TIMESTAMP)", raw_def, re.IGNORECASE)
        if default_match:
            default_val = default_match.group(1)
            if default_val.upper() == "CURRENT_TIMESTAMP":
                kwargs["server_default"] = text("CURRENT_TIMESTAMP")
            else:
                kwargs["server_default"] = text(default_val)

        return Column(col_name, col_type, **kwargs)


    def create_tables(self, engine, table_dict) -> bool:
        """
        Create tables based on the provided dictionary of table names and their corresponding column definitions.

        Args:
            engine (_type_): _database engine object_
            table_dict (_type_): _dictionary of table names and their corresponding column definitions_

        Returns:
            bool: True if tables are created successfully, False otherwise.

        Example:
            >>> from sqlalchemy import create_engine, MetaData, inspect
            >>> engine = create_engine("sqlite:///:memory:")
            >>> table_dict = {
                    "users": [
                        ("username", "VARCHAR(50) NOT NULL"),
                        ("age", "INT"),
                        ("dob", "DATE NOT NULL"),
                        ("current_date", "DATETIME DEFAULT CURRENT_TIMESTAMP")
                    ]
                }
        """
        metadata = MetaData()
        inspector = inspect(engine)

        for table_name, columns in table_dict.items():
            if inspector.has_table(table_name):
                sys.stdout.write(f"✅ Table '{table_name}' already exists. Skipping.")
                continue

            col_objects = []
            for col_name, raw_def in columns:
                col_objects.append(self.parse_column_definition(col_name, raw_def))

            Table(table_name, metadata, *col_objects)
            sys.stdout.write(f"🛠️  Creating table: {table_name} {Table}")

        metadata.create_all(engine)
        self.logger.log("All tables created successfully.")
        return True


    def delete_tables(self, database: str, *table_names: str) -> Optional[Exception]:
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

            URL = self.generate_database_url(credentials=self.CREDENTIALS, database=database)
            engine = create_engine(URL + f"/{database}")
            inspector = inspect(engine)

            if not database_exists(URL + f"/{database}"):
                self.logger.log("Database does not exist", level='error')
                return False

            tables_to_delete = [table for table in table_names if table in inspector.get_table_names()]

            if not tables_to_delete:
                self.logger.log("No specified tables exist to delete", level='error')
                return False

            metadata = MetaData(bind=engine)
            for table_name in tables_to_delete:
                table = Table(table_name, metadata, autoload=True, autoload_with=engine)
                table.drop(bind=engine)
                self.logger.log(f"Table '{table_name}' deleted successfully.")
        except Exception as exception:
            self.logger.log("An error occurred while deleting tables", level='exception')
            return exception


    def insert_columns(self, database_url: str, schema_changes: dict) -> dict:
        """
        Inserts columns into one or more tables if they do not already exist.

        Args:
            database_url (str): Full SQLAlchemy database URL.
            schema_changes (dict): Dictionary where keys are table names and values are lists of tuples.
                                Each tuple contains (column_name, column_definition) as strings.

                                Example:
                                {
                                    "users": [("username", "VARCHAR(50)"), ("age", "INT")],
                                    "orders": [("order_date", "DATETIME DEFAULT CURRENT_TIMESTAMP")]
                                }

        Returns:
            dict: Summary of actions taken for each table.
        """
        results = {}
        engine = create_engine(database_url)
        inspector = inspect(engine)
        try:
            if not database_exists(database_url):
                self.logger.log("Database does not exist", level='exception')
                return {"error": "Database does not exist."}

            with engine.connect() as connection:
                for table, columns in schema_changes.items():
                    if table not in inspector.get_table_names():
                        results[table] = "Table does not exist."
                        continue

                    existing_columns = [col['name'] for col in inspector.get_columns(table)]
                    alter_clauses = []

                    for column_name, column_def in columns:
                        if column_name not in existing_columns:
                            alter_clauses.append(f"ADD COLUMN {column_name} {column_def}")

                    if alter_clauses:
                        try:
                            alter_sql = f"ALTER TABLE {table} " + ", ".join(alter_clauses) + ";"
                            connection.execute(text(alter_sql))
                            results[table] = f"Inserted columns: {[col[0] for col in columns if col[0] not in existing_columns]}"
                            self.logger.log(f"Inserted columns for {table}: {[col[0] for col in columns if col[0] not in existing_columns]}")
                        except Exception:
                            self.logger.log("An error occurred while inserting columns", level='exception')
                        finally:
                            connection.close()
                    else:
                        results[table] = "All columns already exist."

        except Exception as e:
            self.logger.log("An error occurred while inserting columns.", level='exception')
            return {"error": str(e)}

        finally:
            engine.dispose()

        return results


    def delete_columns(self, database: str, table_name: str, column_name: str):
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

            URL = self.generate_database_url(credentials=self.CREDENTIALS, database=database)

            engine = create_engine(URL)
            inspector = inspect(engine)

            if not inspector.has_database(database):
                return {"Error": "Database does not exist!"}

            if not inspector.has_table(table_name):
                return {"Error": "Table does not exist!"}

            command = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
            engine.execute(command)
            self.logger.log(f"Column '{column_name}' deleted successfully from table '{table_name}'.")
            return True
        except SQLAlchemyError as exception:
            self.logger.log(f"An error occurred while deleting the column: {exception}", level='exception')
            return False


    def get_data_from_database(self, database: str = None, table_name: str = None, limit: int = None):
        """
        Retrieves data from a specified database table.

        Args:
            database (str, optional): The name of the database. Defaults to None.
            table_name (str, optional): The name of the table. Defaults to None.
            limit: (int, optional): The number of rows to return.

        Returns:
            list: A list of tuples containing the retrieved data.
            str: An error message if there is an exception.

        Raises:
            ProgrammingError: If there is an exception while executing the SQL query.
        """
        if not database or not table_name:
            return "Please provide both database and table name as arguments to the function. \nExample: get_data_from_database(database='food_db', table_name='food_recipes')"

        try:

            URL =  self.generate_database_url(self.CONFIG, database=database)
            with create_engine(URL).connect() as connection:
                if connection:
                    databases = [row[0] for row in connection.execute(text("SHOW DATABASES")).fetchall()]
                    if database in databases:
                        tables = [ row[0] for row in connection.execute(text("SHOW TABLES IN " + database)).fetchall()]
                        if table_name in tables:
                            if limit:
                                return connection.execute(text(f"SELECT * FROM {database}.{table_name} LIMIT {limit}")).fetchall()
                            return connection.execute(text(f"SELECT * FROM {database}.{table_name}")).fetchall()
                        else:
                            return f"""The table '{table_name}' does not exist. Trying with capitalized table name. {connection.execute(text(f"SELECT * FROM {database}.{table_name.upper()}")).fetchall()}"""
                    else:
                        return f"The database '{database}' does not exist. Try with capitalized database name. {databases}"

        except ProgrammingError as exception:
            return exception


    def generate_database_url(self, credentials: dict, database: str) -> str:
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
        self.logger.log("Generating database URL")
        return f"{credentials.get('connector')}://{credentials.get('user')}:{credentials.get('password')}@{credentials.get('hostname')}/{database}"


    def insert_dataframe(self, database: str, table_name: str, dataframe: pd.DataFrame):
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
        dataframe = self.delete_duplicates(dataframe)
        sys.stdout.write("Dataframe created successfully!")
        URL =  self.generate_database_url(self.get_credentials(), database)
        sys.stdout.write("Database URL generated successfully!")
        try:
            if database_exists(URL):
                with create_engine(URL).connect() as connection:
                    if not database_exists(URL):
                        self.create_database_function(database)
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0 and dataframe is not None:
                        dataframe.to_sql(name=table_name, con=create_engine(URL), if_exists='replace', index=False)
                        self.logger.log(f"Dataframe inserted successfully into table '{table_name}' in database '{database}'.")
                        return {200: "Dataframe inserted successfully!"}
                    else:
                        self.logger.log(f"Table '{table_name}' already exists in database '{database}'.")
                        return {500: "Dataframe already exists!"}
            else:
                self.create_database_function(database)
                self.logger.log(f"Database '{database}' created successfully.")
                self.logger.log(f"Inserting dataframe into table '{table_name}' in database '{database}'.")
                return self.insert_dataframe(database=database, table_name=table_name, dataframe=dataframe)
        except ProgrammingError as e:
            self.logger.log(f"An error occurred while inserting the dataframe: {e}", level='exception')
            return e


    def add_new_data_to_table(self, database: str = None, table_name: str = None, dataframe: pd.DataFrame = None):
        """
        Adds new data to a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.

        Returns:
            None
        """
        URL =  self.generate_database_url(credentials=self.CREDENTIALS, database=database)
        try:
            if database_exists(URL):
                if dataframe and isinstance(dataframe, pd.DataFrame):
                    existing_data = pd.read_sql_table(table_name, con=create_engine(URL))
                    if existing_data.keys() == dataframe.keys():
                        dataframe.to_sql(name=table_name, con=create_engine(URL), if_exists='append', index=False)
                    else:
                        return None
                new_row = {}
                existing_data = pd.DataFrame(pd.read_sql_table(table_name, con=create_engine(URL)))
                for column in existing_data.columns:
                    new_row[column] = input(f"Enter value for {column}: ")
                new_row_df = pd.DataFrame(new_row, index=[0])
                updated_data = pd.concat([existing_data, new_row_df], ignore_index=True)
                updated_data = updated_data.drop_duplicates()
                updated_data.to_sql(name=table_name, con=create_engine(URL), if_exists='replace', index=False)
                self.logger.log(f"Dataframe inserted successfully into table '{table_name}' in database '{database}'.")
                return {200: "Dataframe inserted successfully!"}
            else:
                self.logger.log(f"Database '{database}' does not exist.", level='error')
                return False
        except Exception as e:
            self.logger.log(e, level='exception')
            raise e


    def modify_column(self, database: str, table_name: str, column_name: str, command: str) -> bool:
        """
        Modifies a column in a database table.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            column_name (str): The name of the column.
            command (str): The modification command to execute.

        Returns:
            bool: If the modification is successful, return True, else return False.
        """
        try:
            URL =  self.generate_database_url(credentials=self.CREDENTIALS, database=database)

            with create_engine(URL + f"/{database}").connect() as connection:
                inspector = inspect(connection)
                if database_exists(URL + f"/{database}"):
                    if table_name in inspector.get_table_names():
                        command = f"ALTER TABLE {table_name} MODIFY {column_name} {command}"
                        connection.execute(command)
                        self.logger.log(f"Column '{column_name}' in table '{table_name}' modified successfully.")
                        return True
                    else:
                        self.logger.log(f"Table '{table_name}' does not exist in database '{database}'.", level='error')
                        return False
                else:
                    self.logger.log(f"Database '{database}' does not exist.", level='error')
                    return False
        except Exception as exception:
            self.logger.log(f"An error occurred: {exception}", level='exception')
            return exception


    def inspect_columns(self, database: str, table: str, *column: str) -> list | str | Exception:
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

            URL =  self.generate_database_url(credentials = self.CREDENTIALS, database=database)
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


    def query(self, database: str, table_name: str, filter_condition: str) -> list | Exception:
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

            URL =  self.generate_database_url(credentials = self.CREDENTIALS, database=database)
            with create_engine(f"{URL}/{database}").connect() as connection:
                inspector = inspect(connection)
                if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
                    return connection.execute(f"SELECT * FROM {table_name} WHERE {filter_condition}").fetchall()
        except Exception as exception:
            return exception


    def check_for_duplicates(self, database: str, table_name: str, column_name: str) -> list:
        """
        Check for duplicates in a specific column of a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            column_name (str): The name of the column to check for duplicates.

        Returns:
            list: A list of rows representing the duplicate values found in the column.
        """


        URL = self.generate_database_url(credentials = self.CREDENTIALS, database=database)
        if database_exists(URL):
            with create_engine(f"{URL}/{database}").connect() as connection:
                inspector = inspect(connection)
                if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
                    query = f"SELECT {column_name} FROM {table_name} GROUP BY {column_name} HAVING COUNT({column_name}) > 1"
                    return connection.execute(query).fetchall()


    def delete_duplicates(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Delete duplicates from the given dataframe.

        Args:
            dataframe (pd.DataFrame): The dataframe to remove duplicates from.

        Returns:
            pd.DataFrame: The dataframe with duplicates removed.
        """
        if os.path.isdir(dataframe):
            for file in os.listdir(dataframe):
                if file.endswith(".csv"):
                    dataframe  = self.delete_duplicates(os.path.join(dataframe, file))
        if isinstance(dataframe, pd.DataFrame):
            self.logger.log(dataframe)
            self.logger.log("Not a pandas dataframe, trying to convert to pandas dataframe\n", level='warning')
            dataframe = pd.DataFrame(pd.read_csv(dataframe, encoding="cp1252"))
            dataframe = dataframe.drop_duplicates()
            return dataframe
        else:
            dataframe = dataframe.drop_duplicates()
            return dataframe


    def add_pk(self, database: str, table_name: str, constraint_name: str, column_name: str) -> bool:
        """
        Add constraints to a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            constraint_name (str): The name of the constraint.
            column_name (str): The name of the column to be constrained.

        Returns:
            bool: True if the constraint is added successfully, False otherwise.
        """
        URL =  self.generate_database_url(credentials=self.get_credentials(), database=database)
        try:
            if database_exists(URL):
                with create_engine(URL).connect() as connection:
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        self.logger.log(f"The table '{table_name}' does not exist!\n", level='error')
                        return False
                    else:
                        existing_data = pd.read_sql_table(table_name, con=create_engine(URL))
                        columns = existing_data.columns
                        if column_name in [column for column in columns]:
                            connection.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY (`{column_name}`)"))
                            self.logger.log(f"Primary key added successfully to column '{column_name}' in table '{table_name}' in database '{database}'.")
                            return True
                        else:
                            self.logger.log(f"The column '{column_name}' does not exist in table '{table_name}'.", level='error')
                            return False
            else:
                self.logger.log(f"The database '{database}' does not exist!\n", level='error')
                return False
        except Exception as e:
            self.logger.log(f"An error occurred while adding the primary key: {e}", level='exception')
            return False


    def delete_pk(self, database: str, table_name: str) -> bool:
        """
        Delete constraints from a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            constraint_name (str): The name of the constraint.

        Returns:
            bool: True if the constraint is deleted successfully, False otherwise.
        """
        URL =  self.generate_database_url(credentials=self.get_credentials(), database=database)
        try:
            if database_exists(URL):
                with create_engine(URL).connect() as connection:
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        self.logger.log(f"The table '{table_name}' does not exist!\n", level='error')
                        return False
                    else:
                        connection.execute(text(f"ALTER TABLE {table_name} DROP PRIMARY KEY"))
                        self.logger.log(f"Primary key deleted successfully from table '{table_name}' in database '{database}'.")
                        return True
            else:
                self.logger.log(f"The database '{database}' does not exist!\n", level='error')
                return False
        except ProgrammingError as e:
            self.logger.log(f"An error occurred while deleting the primary key: {e}", level='exception')


    def upload_dataset_to_database(self, database: str = None, table_name: str = None, dataset: str = None, user: str = None, dataset_path: str = None) -> bool:
        """
        Uploads a dataset to a database table.

        Parameters:
            database (str): The name of the database.
            table_name (str): The name of the table in the database.
            dataset (str): The name of the dataset to upload.
            user (str): The name of the user who owns the dataset.
            dataset_path (str): The path to the dataset on the local machine.

        Returns:
            bool: True if the dataset is uploaded successfully, False otherwise.
        """
        dataset_path = self.CREDENTIALS['default_download_folder'] + "/datasets"
        if os.path.exists(dataset_path):
            sys.stdout.write(f"The folder '{dataset_path}' does not exist!\n")
            return False
        try:
            for file in os.listdir(dataset_path + f"/{str(dataset).split('/')[-1]}"):
                sys.stdout.write(file)
                self.insert_dataframe(database=database, table_name=table_name, dataframe=dataset_path + f"/{str(dataset).split('/')[-1]}/" + file)
                self.logger.log(f"Dataset '{dataset}' uploaded successfully to table '{table_name}' in database '{database}'.")
            return True
        except Exception as e:
            self.logger.log(f"An error occurred while uploading the dataset: {e}", level='exception')
            return False


    def download_dataset_from_database(self, database: str, table_name: str, download_path: str) -> None:
        """
        Downloads a dataset from a database table.

        Parameters:
            database (str): The name of the database.
            table_name (str): The name of the table in the database.
            download_path (str): The path to download the dataset to.

        Returns:
            bool: True if the dataset is downloaded successfully, False otherwise.
        """

        URL = self.generate_database_url(credentials = self.CREDENTIALS, database=database)
        try:
            if database_exists(URL):
                with create_engine(URL).connect() as connection:
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        self.logger.log(f"The table '{table_name}' does not exist!\n", level='error')
                        return False
                    else:
                        pd.read_sql_table(table_name, con=create_engine(URL)).to_csv(self.CREDENTIALS['default_download_folder'] + "/" + download_path)
                        self.logger.log(f"Dataset from table '{table_name}' in database '{database}' downloaded successfully to '{download_path}'.")
                        return True
            else:
                self.logger.log(f"The database '{database}' does not exist!\n", level='error')
                return False
        except Exception as e:
            self.logger.log(f"An error occurred while downloading the dataset: {e}", level='exception')
            return False


# pprint(kaggle_handler.search_kaggle_datasets_with_keyword(keyword='vegetables'))
# TODO: Fix encrypt and decrypt function
# TODO: Add AWS Support. Think of GCP and Azure