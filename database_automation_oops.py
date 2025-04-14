import pandas as pd
import yaml
import os
import logging
import boto3
import argparse
import sys
from configparser import ConfigParser
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database, database_exists, drop_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import ProgrammingError
from cryptography.fernet import Fernet


class DataBaseHandler():
    def __init__(self, username: str = None):
        self.METADATA = MetaData()

        self.HOME = os.path.expanduser('~')
        self.USERNAME = username.lower().replace(" ", "_") or input("Enter your username: ").replace(" ", "_").lower()
        self.log_file_name = f"{self.HOME}/databse_handler.log" or input(f"Enter the path to your log file. Defaults to {self.HOME}/.database_handler.log: ")
        logging.basicConfig(
            level=logging.INFO,
            filemode="a",
            filename=self.log_file_name,
            format="%(asctime)s;%(levelname)s;%(message)s",
        )
        if os.path.exists(os.path.join(self.HOME, ".aws/credentials")):
            self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
            self.SESSION = boto3.Session(profile_name=self.USERNAME)
            self.CONFIG = self.SESSION.get_credentials()
        elif os.path.exists(os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml")):
            self.CONFIG_PATH = os.path.join(self.HOME, f".{self.USERNAME}_database_credentials")
            with open(os.path.join(self.HOME, f".{self.USERNAME}_database_credentials.yaml"), "r") as file:
                self.CONFIG = yaml.safe_load(file)
        else:
            self.CONFIG_PATH = None
        if self.CONFIG_PATH is None:
            self.CONFIG_PATH, self.CONFIG = self.create_credentials_file()


    def get_args(self):
        """
        Read the command line arguments and return them.

        Args:
            None

        Returns:
            None
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


    def get_credentials(self):
        """
        Read the YAML credentials file and return its contents.

        :return: A dictionary containing the contents of the YAML file.
        """
        if self.CONFIG_PATH is None:
            self.create_credentials_file()
        elif ".aws" in self.CONFIG_PATH:
            config = ConfigParser()
            config.read(self.CONFIG_PATH)
            return self.CONFIG_PATH, config
        else:
            with open(self.CONFIG_PATH, "r") as file:
                yaml_file = yaml.safe_load(file)
                return self.CONFIG_PATH, yaml_file


    def create_credentials_file(self):
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
                self.aws_access_key_id = args.aws_access_key or input("Enter your AWS access key ID: ")
                self.aws_secret_access_key = args.aws_secret_key or input("Enter your AWS secret access key: ")
                self.region_name = args.aws_region or input("Enter the region name. Defaults to us-east-1: ") or "us-east-1"
                if not os.path.exists(os.path.join(self.HOME, ".aws")):
                    os.makedirs(os.path.join(self.HOME, ".aws"), exist_ok=True)
                self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
                with open(self.CONFIG_PATH, 'w') as file:
                    self.CONFIG[self.USERNAME] = {
                                            "aws_access_key_id": self.aws_access_key_id,
                                            "aws_secret_access_key": self.aws_secret_access_key,
                                            "region": self.region_name
                                        }
                    self.CONFIG.write(file)
                logging.info(f"Credentials file created in {self.CONFIG_PATH}")
                return (self.CONFIG_PATH, self.CONFIG)
            else:
                self.warning_choice = input("Warning! You are usinga local database and hence won't get cross device functionality and device sharing. Are you ok to continue? (y/n): ")
                while self.warning_choice.lower() not in ["y", "n"]:
                    self.warning_choice = input("Warning! You are using a local database and hence won't get cross device functionalit. Are you ok to continue? (y/n): ")
                if self.warning_choice == "n":
                    return "Not creating the database. Exiting..."
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
                logging.info(f"Credentials file created in {self.CONFIG_PATH}")
                return (self.CONFIG_PATH, self.CONFIG)
        elif args.database_type == "aws":
            self.aws_access_key_id = args.aws_access_key or input("Enter your AWS access key ID: ")
            self.aws_secret_access_key = args.aws_secret_key or input("Enter your AWS secret access key: ")
            self.region_name = args.aws_region or input("Enter the region name. Defaults to us-east-1: ") or "us-east-1"
            if not os.path.exists(os.path.join(self.HOME, ".aws")):
                os.makedirs(os.path.join(self.HOME, ".aws"), exist_ok=True)
            self.CONFIG_PATH = os.path.join(self.HOME, ".aws/credentials")
            with open(self.CONFIG_PATH, 'w') as file:
                self.CONFIG[self.USERNAME] = {
                                        "aws_access_key_id": self.aws_access_key_id,
                                        "aws_secret_access_key": self.aws_secret_access_key,
                                        "region": self.region_name
                                    }
                self.CONFIG.write(file)
            self.SESSION = boto3.Session(profile_name=self.USERNAME)
            self.CONFIG = self.SESSION.get_credentials()
            logging.info(f"Credentials file created in {self.CONFIG_PATH}")
            return (self.CONFIG_PATH, self.CONFIG)
        else:
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
            data = {self.USERNAME.replace(' ', '_').lower(): {

                "user": username,
                "password": password,
                "hostname": host,
                "port": port,
                "connector": connector,
            }}
            with open(self.CONFIG_PATH, 'w') as file:
                yaml.safe_dump(data, file)
            logging.info(f"Credentials file created in {self.CONFIG_PATH}")
            return (self.CONFIG_PATH, self.CONFIG)


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


    def create_database_function(self, database: str):
        """
        Create a database function.

        Args:
            database (str): The name of the database to be created.

        Returns:
            dict: A dictionary containing the status of the database creation. If the database is created successfully, the dictionary will have the key "Ok" and the value "Database Created!". If the database already exists, the dictionary will have the key "Already Exists!" and the value "Database Already Exists!".
            tuple: If an exception occurs during the creation of the database, a tuple with None as the first element and the exception as the second element will be returned.
        """
        try:

            URL = f"{self.CONFIG.get(self.USERNAME).get('connector')}://{self.CONFIG.get(self.USERNAME).get('user')}:{self.CONFIG.get(self.USERNAME).get('password')}@{self.CONFIG.get(self.USERNAME).get('hostname')}"
            engine = create_engine(URL)
            connection = engine.connect()
            if not database_exists(f"{URL}/{database}"):
                connection.execute(create_database(f"{URL}/{database}"))
                return {"Ok": "Database Created!"}
            return {"Already Exists!": "Database Already Exists!"}
        except Exception as exception:
            return (None, exception)


    def delete_database_function(self,database: str):
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

            URL = f"{self.CONFIG.get(self.USERNAME).get('connector')}://{self.CONFIG.get(self.USERNAME).get('user')}:{self.CONFIG.get(self.USERNAME).get('password')}@{self.CONFIG.get(self.USERNAME).get('hostname')}"
            with create_engine(URL).connect() as connection:
                if database_exists(f"{URL}/{database}"):
                    connection.execute(drop_database(f"{URL}/{database}"))
                    return {"Ok": "Database Deleted!"}
                else:
                    return {"Doesn't Exist!": "Database doesn't Exist!"}
        except Exception as exception:
            return exception


    def create_tables(self, database: str, *table_names: str, column_name: str = None):
        """
        Creates tables in a specified database.

        Parameters:
            database (str): The name of the database to create tables in.
            *table_names (str): Variable length argument list of table names to create.

        Returns:
            str or dict: If the tables are successfully created, returns 'Table already exists!' if the table already exists in the database. If the database does not exist, returns a dictionary with the key 'Error!' and the value 'Database does not exist!'. If an exception occurs during the table creation process, returns the exception object.
        """
        try:

            USER = self.CONFIG.get(self.USERNAME).get('user')
            PASSWORD = self.CONFIG["password"]
            HOSTNAME = self.CONFIG.get('hotname')
            CONNECTOR = self.CONFIG.get(self.USERNAME).get('connector')
            URL = f"{CONNECTOR}://{USER}:{PASSWORD}@{HOSTNAME}/{database}"
            engine = create_engine(URL, echo=True)
            inspector = inspect(engine)

            if database_exists(URL):
                for table_name in table_names:
                    if table_name not in inspector.get_table_names():
                        if column_name:
                            table = Table(table_name, self.METADATA, Column('Id', Integer, primary_key=True), Column(column_name, Integer))
                            table.create(bind=engine)
                        else:
                            table = Table(table_name, self.METADATA, Column('Id', Integer, primary_key=True))
                            table.create(bind=engine)
                    else:
                        return 'Table already exists!'
            else:
                return {'Error!': 'Database does not exist!'}
        except Exception as e:
            return e


    def delete_tables(self, database: str, *table_names: str):
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

            URL = f"{self.CONFIG.get(self.USERNAME).get('connector')}://{self.CONFIG.get(self.USERNAME).get('user')}:{self.CONFIG.get(self.USERNAME).get('password')}@{self.CONFIG.get(self.USERNAME).get('hostname')}"
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


    def insert_columns(self, database: str, table_name: str, column_name: str, datatype: str, size: str, command: str):
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

            USER = self.CONFIG.get(self.USERNAME).get('user')
            PASSWORD = self.CONFIG["PASSWORD"]
            HOSTNAME = self.CONFIG.get('hotname')
            connector = self.CONFIG.get(self.USERNAME).get('connector')
            URL = f"{connector}://{USER}:{PASSWORD}@{HOSTNAME}/{database}"

            engine = create_engine(URL)
            inspector = inspect(engine)

            if database_exists(URL) and table_name in inspector.get_table_names():
                command = text(f'ALTER TABLE {table_name} ADD {column_name} {datatype}({size}) {command}')
                engine.execute(command)
                return 'Successfully inserted columns!'
        except Exception as exception:
            return exception


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

            URL = f"{self.CONFIG.get(self.USERNAME).get('connector')}://{self.CONFIG.get(self.USERNAME).get('user')}:{self.CONFIG.get(self.USERNAME).get('password')}@{self.CONFIG.get(self.USERNAME).get('hostname')}/{database}"

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


    def get_data_from_database(self, database: str = None, table_name: str = None):
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

            url = self.generate_database_url(self.CONFIG, database=database)
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


    def generate_database_url(self, credentials: dict, database: str):
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
        print("Dataframe created successfully!")
        url = self.generate_database_url(self.get_credentials(), database)
        print("Database URL generated successfully!")
        try:
            if database_exists(url):
                with create_engine(url).connect() as connection:
                    if not database_exists(url):
                        self.create_database_function(database)
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0 and dataframe is not None:
                        dataframe.to_sql(name=table_name, con=create_engine(url), if_exists='replace', index=False)
                        return {200: "Dataframe inserted successfully!"}
                    else:
                        return {500: "Dataframe already exists!"}
            else:
                self.create_database_function(database)
                return self.insert_dataframe(database=database, table_name=table_name, dataframe=dataframe)
        except ProgrammingError as e:
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
        url = self.generate_database_url(self.get_credentials(), database=database)
        try:
            if database_exists(url):
                if dataframe and isinstance(dataframe, pd.DataFrame):
                    existing_data = pd.read_sql_table(table_name, con=create_engine(url))
                    if existing_data.keys() == dataframe.keys():
                        dataframe.to_sql(name=table_name, con=create_engine(url), if_exists='append', index=False)
                    else:
                        return None
                new_row = {}
                existing_data = pd.DataFrame(pd.read_sql_table(table_name, con=create_engine(url)))
                for column in existing_data.columns:
                    new_row[column] = input(f"Enter value for {column}: ")
                new_row_df = pd.DataFrame(new_row, index=[0])
                updated_data = pd.concat([existing_data, new_row_df], ignore_index=True)
                updated_data = updated_data.drop_duplicates()
                updated_data.to_sql(name=table_name, con=create_engine(url), if_exists='replace', index=False)
                return {200: "Dataframe inserted successfully!"}
            else:
                return None
        except Exception as e:
            logging.error(e)
            raise e


    def modify_column(self, database: str, table_name: str, column_name: str, command: str):
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
            credentials = self.get_credentials()
            URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"

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


    def inspect_columns(self, database: str, table: str, *column: str):
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
            credentials = self.get_credentials()
            URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
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


    def query(self, database: str, table_name: str, filter_condition: str):
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
            credentials = self.get_credentials()
            URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
            with create_engine(f"{URL}/{database}").connect() as connection:
                inspector = inspect(connection)
                if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
                    return connection.execute(f"SELECT * FROM {table_name} WHERE {filter_condition}").fetchall()
        except Exception as exception:
            return exception


    def check_for_duplicates(self, database: str, table_name: str, column_name: str):
        """
        Check for duplicates in a specific column of a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            column_name (str): The name of the column to check for duplicates.

        Returns:
            list: A list of rows representing the duplicate values found in the column.
        """

        credentials = self.get_credentials()
        URL = f"{credentials['CONNECTOR']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOSTNAME']}"
        with create_engine(f"{URL}/{database}").connect() as connection:
            inspector = inspect(connection)
            if database_exists(f"{URL}/{database}") and table_name in inspector.get_table_names():
                query = f"SELECT {column_name} FROM {table_name} GROUP BY {column_name} HAVING COUNT({column_name}) > 1"
                return connection.execute(query).fetchall()


    def delete_duplicates(self, dataframe: pd.DataFrame):
        """
        Delete duplicates from the given dataframe.

        Args:
            dataframe (pd.DataFrame): The dataframe to remove duplicates from.

        Returns:
            pd.DataFrame: The dataframe with duplicates removed.
        """
        if os.path.isdir(dataframe):
            print(True)
            for file in os.listdir(dataframe):
                if file.endswith(".csv"):
                    print(file)
                    dataframe  = self.delete_duplicates(os.path.join(dataframe, file))
        if isinstance(dataframe, pd.DataFrame):
            print(dataframe)
            sys.stdout.write("Not a pandas dataframe, trying to convert to pandas dataframe\n")
            dataframe = pd.DataFrame(pd.read_csv(dataframe, encoding="cp1252"))
            dataframe = dataframe.drop_duplicates()
            return dataframe
        else:
            dataframe = dataframe.drop_duplicates()
            return dataframe


    def add_pk(self, database: str, table_name: str, constraint_name: str, column_name: str, delete_constraint: bool):
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
        url = self.generate_database_url(self.get_credentials(), database=database)
        try:
            if database_exists(url):
                with create_engine(url).connect() as connection:
                    if not database_exists(url):
                        self.create_database_function(database)
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        sys.stdout.write(f"The table '{table_name}' does not exist!\n")
                        return None
                    else:
                        existing_data = pd.read_sql_table(table_name, con=create_engine(url))
                        columns = existing_data.columns
                        if column_name in [column for column in columns]:
                            connection.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY (`{column_name}`)"))
                            sys.stdout.write("Constraint added successfully!\n")
                        else:
                            sys.stdout.write(f"Column does not exist!\nHere are the existing columns: {[column for column in columns]}\n")
                            return None
            else:
                sys.stdout.write(f"The database '{database}' does not exist!\n")
                return None
        except Exception as e:
            sys.stdout.write(str(e))


    def delete_pk(self, database: str, table_name: str):
        """
        Delete constraints from a table in a given database.

        Args:
            database (str): The name of the database.
            table_name (str): The name of the table.
            constraint_name (str): The name of the constraint.

        Returns:
            None
        """
        url = self.generate_database_url(self.get_credentials(), database=database)
        try:
            if database_exists(url):
                with create_engine(url).connect() as connection:
                    if not database_exists(url):
                        self.create_database_function(database)
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        sys.stdout.write(f"The table '{table_name}' does not exist!\n")
                        return None
                    else:
                        connection.execute(text(f"ALTER TABLE {table_name} DROP PRIMARY KEY"))
                        sys.stdout.write("PRIMARY KEY deleted successfully!\n")
            else:
                sys.stdout.write(f"The database '{database}' does not exist!\n")
                return None
        except ProgrammingError as e:
            sys.stdout.write(str(e) + "\n")


    def upload_dataset_to_database(self, database: str = None, table_name: str = None, dataset: str = None, user: str = None, dataset_path: str = None):
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
        credentials = self.get_credentials()
        dataset_path = credentials['default_download_folder'] + "/datasets"
        if os.path.exists(dataset_path):
            sys.stdout.write(f"The folder '{dataset_path}' does not exist!\n")
            return None
        for file in os.listdir(dataset_path + f"/{str(dataset).split('/')[-1]}"):
            sys.stdout.write(file)
            self.insert_dataframe(database=database, table_name=table_name, dataframe=dataset_path + f"/{str(dataset).split('/')[-1]}/" + file)


    def download_dataset_from_database(self, database: str, table_name: str, download_path: str):
        """
        Downloads a dataset from a database table.

        Parameters:
            database (str): The name of the database.
            table_name (str): The name of the table in the database.
            download_path (str): The path to download the dataset to.

        Returns:
            None
        """
        credentials = self.get_credentials()
        url = self.generate_database_url(credentials, database=database)
        try:
            if database_exists(url):
                with create_engine(url).connect() as connection:
                    if not database_exists(url):
                        self.create_database_function(database)
                    if len(connection.execute(text(f"SHOW TABLES IN {database}")).fetchall()) == 0:
                        sys.stdout.write(f"The table '{table_name}' does not exist!\n")
                        return None
                    else:
                        pd.read_sql_table(table_name, con=create_engine(url)).to_csv(credentials['default_download_folder'] + "/" + download_path)
            else:
                sys.stdout.write(f"The database '{database}' does not exist!\n")
                return None
        except Exception as e:
            sys.stdout.write(str(e) + "\n")
            return None


# Step 1: Define the database connection URL (use SQLite for simplicity)
DATABASE_URL = "sqlite:///example.db"  # This will create a local SQLite database

# Step 2: Create an engine
engine = create_engine(DATABASE_URL, echo=True)

# Step 3: Create a base class for table definitions
Base = declarative_base()

# Step 4: Define your table as a Python class
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)

# Step 5: Create the table in the database
Base.metadata.create_all(engine)


if __name__ == "__main__":
    pass




# TODO: Fix encrypt and decrypt function
# TODO: Add AWS Support. Think of GCP and Azure