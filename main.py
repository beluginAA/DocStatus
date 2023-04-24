import pandas as pd
import pyodbc
import warnings
import sys

from loguru import logger
from tkinter.filedialog import askopenfilename
from preprocessing import Preproccessing, PostProcessing


warnings.simplefilter(action = 'ignore', category = (UserWarning, FutureWarning))

logger.remove()
mainLogger = logger.bind(name = 'mainLogger').opt(colors = True)
mainLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)
mainLogger.info('Programm starts.')

databaseRoot = askopenfilename(title = 'Select database to edit', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
connect = Preproccessing(databaseRoot)
statusDf, docDf = connect.to_database()

mainLogger.info('Preparing data for merging.')
statusDf['Ревизия'] = statusDf['Ревизия'].apply(lambda row: row  if pd.isna(row) or row == 0 else f'C0{row}')
docDf['Ревизия'] = docDf['Ревизия'].apply(lambda row: row[:3]  if '(есть только в 1С)' in str(row) else row)

mainLogger.info('Merging two databases.')
mergedDf = pd.merge(docDf, statusDf,
                    how = 'outer',
                    on = ['Шифр', 'Ревизия'],
                    suffixes = ['', '_new'],
                    indicator = True)
mergedDf.to_excel('test.xlsx', index = False)

