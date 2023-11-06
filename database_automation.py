from sqlalchemy import create_engine, Table, Column, Integer, MetaData, inspect, text
from sqlalchemy_utils import create_database, database_exists, drop_database

USER = "root"
PASSWORD = "MYmac123"
HOSTNAME = "localhost"
METADATA = MetaData()
URL = f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}"

def create_database_function(database: str):
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
    try:
        engine = create_engine(URL+ f"/{database}")
        inspector = inspect(engine)
        table = Table(f"{table_name}", MetaData(bind=engine), autoload=True, autoload_with=engine)
        if database_exists(URL+f"/{database}"):
            if table in inspector.get_table_names():
                engine.execute(f"SELECT * FROM {table} WHERE {filter_condition}").fetchall()
    except Exception as exception:
        return exception
