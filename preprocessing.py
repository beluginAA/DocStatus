import pandas as pd
import pyodbc
import sys

from loguru import logger


class Preproccessing:

    preLogger = logger.bind(name = 'preLogger').opt(colors = True)
    preLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str):
        self.connStr = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={databaseRoot};'
            )

    def to_database(self) -> pd.DataFrame :
        Preproccessing.preLogger.info('Trying to connect to a database.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                statusDatabaseQuery = '''SELECT * FROM [Переданные_РД]'''
                docDatabaseQuery = '''SELECT * FROM [Документация]'''
                statusDatabase = pd.read_sql(statusDatabaseQuery, connection)
                docDatabase = pd.read_sql(docDatabaseQuery, connection)
        except Exception as e:
            Preproccessing.preLogger.error(f"An error occurred while connecting to the database: {e}")
        else:
            Preproccessing.preLogger.info('--The connection to the database was successful.--')
            return statusDatabase, docDatabase
        


class PostProcessing:

    postLogger = logger.bind(name = 'postLogger').opt(colors = True)
    postLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str):
        self.connStr = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={databaseRoot};'
                )

    def delete_table(self) -> None:
        PostProcessing.postLogger.info('Trying to delete an old table.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE [Документация]")
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while deleting the table: {e}")
        else:
            PostProcessing.postLogger.info('--An old table has been successfully deleted.--')
    
    def create_table(self) -> None:
        PostProcessing.postLogger.info('Trying to create a new table.')
        createTableQuery = '''CREATE TABLE [Документация] ([Система] VARCHAR(200), 
                                            [Наименование] VARCHAR(200),
                                            [Шифр] VARCHAR(100),
                                            [Разработчик] VARCHAR(200),
                                            [Вид] VARCHAR(100),
                                            [Тип] VARCHAR(200),
                                            [Статус] VARCHAR(200),
                                            [Ревизия] VARCHAR(200), 
                                            [Дополнения] VARCHAR(200),
                                            [Срок] VARCHAR(100),
                                            [Сервер] VARCHAR(200),
                                            [Обоснование] VARCHAR(200))'''
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(createTableQuery)
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while creating the table: {e}")
        else:
            PostProcessing.postLogger.info('--An old table has been successfully created.--')
    
    def insert_into_table(self, dataframe:pd.DataFrame) -> None:
        PostProcessing.postLogger.info('Trying to insert new data into new table.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                for row in dataframe.itertuples(index=False):
                    insertQuery = f'''INSERT INTO [Документация] ([Система], [Наименование],
                                            [Шифр], [Разработчик],
                                            [Вид], [Тип],
                                            [Статус], [Ревизия], 
                                            [Дополнения], [Срок],
                                            [Сервер], [Обоснование]) 
                                            VALUES ({",".join(f"'{x}'" for x in row)})'''
                    cursor.execute(insertQuery)
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while inserting the data: {e}")
        else:
            PostProcessing.postLogger.info('--Data was successfully added to the table.--')
