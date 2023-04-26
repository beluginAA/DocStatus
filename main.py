import pandas as pd
import pyodbc
import warnings
import sys

from loguru import logger
from tkinter.filedialog import askopenfilename
from preprocessing import Preproccessing, PostProcessing


def get_status_server(df):
    if df['_merge'] == 'both':
        return 'Выложен'
    else:
        return 'Не выложен'

warnings.simplefilter(action = 'ignore', category = (UserWarning, FutureWarning))

logger.remove()
mainLogger = logger.bind(name = 'mainLogger').opt(colors = True)
mainLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)
mainLogger.info('Programm starts.')

databaseRoot = askopenfilename(title = 'Select database to edit', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
connect = Preproccessing(databaseRoot)
statusDf, docDf = connect.to_database()

mainLogger.info('Preparing data for merging.')
# поправить замечания по поводу ревизии (после merge вернуть значений (есть только в 1С)).Программа записывает все в БД корректно, просто нужно вернуть некоторые значнения
statusDf['Ревизия'] = statusDf['Ревизия'].apply(lambda row: row  if pd.isna(row) or row == 0 else f'C0{row}')
docDf['Ревизия'] = docDf['Ревизия'].apply(lambda row: row[:3]  if '(есть только в 1С)' in str(row) else row)

mainLogger.info('Merging two databases.')
mergedDf = pd.merge(docDf, statusDf,
                    how = 'outer',
                    on = ['Шифр', 'Ревизия'],
                    suffixes = ['', '_new'],
                    indicator = True)
mergedDf['Сервер'] = mergedDf.apply(get_status_server, axis = 1)

mainLogger.info('Preparing summary dataframe.')
summaryDf = mergedDf[mergedDf['_merge'].isin(['both', 'left_only'])]
summaryDf = summaryDf[docDf.columns]

mainLogger.info('Making changes to the database.')
attempt = PostProcessing(databaseRoot)
attempt.delete_table()
attempt.create_table()
attempt.insert_into_table(summaryDf)

mainLogger.info('Database updated.')



