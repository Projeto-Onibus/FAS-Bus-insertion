"""
    PopulateDatabase.py
    Autor: Fernando Dias
    Descrição: Faz o preenchimento inicial do banco de dados com os arquivos .tar.gz de forma eficiente. O maior gargalo é a criacao dos arquivos md5 a partir do .tar.gz
"""
import io
import os
import sys
import datetime
import logging
from configparser import ConfigParser


import psycopg2 as db

# from .main import ConfigurationScript

from data_handling import ImportBusData
from data_handling import ImportLineData


def PopulateDbInitial(desiredDate,database,CONFIGS,log):
    log.debug(f"Gathering data from files")
    processed = [False]
    _, busTable = ImportBusData.GatherBusData(desiredDate,CONFIGS,logging,processedStatus=processed)
    # TODO: Trocar esse metodo de "salvar estado" para um que nao seja dependente do arquivo
    if processed[0]:
        log.info(f"Data already processed, skipping...")
        return None
    log.debug("Converting to CSV")
    with io.StringIO() as buffer:
        buffer.write(busTable[['time','id','lat','lon','speed','line']].to_csv(na_rep='\\N',index=False,header=False,line_terminator=",\\N\n"))
        buffer.seek(0)
        log.debug("Sending to database")
        with database.cursor() as cursor:
            cursor.copy_from(buffer,'bus_data_simple',sep=',')
            cursor.execute("SELECT COUNT(*) FROM bus_data")
            results = cursor.fetchall()
            log.debug(f"written {results[0][0]} new entries")
            database.commit()

def mainBus(availableDays,database,CONFIGS,logger):
    total = len(availableDays)
    now = 0
    for desiredDate in availableDays:
        logger.debug(f"current date: {desiredDate}")
        PopulateDbInitial(desiredDate,database,CONFIGS,logger)
        now += 1
        logger.info(f"done {now*100/total:.1f}%")


def mainLines(database,CONFIGS,log):
    error, lineTables = ImportLineData.GatherLineData(CONFIGS,log)
    if error:
        log.critical(error)
        exit()
    
    #ImportLineData.GetLineDataFromAPI(CONFIGS,log)
    lineMap,lineData = (lineTables['map'],lineTables['data'])
   
    with database.cursor() as cursor:
        cursor.executemany("INSERT INTO line_registered(line_id,direction,description) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",lineMap[['line','direction','description']].values.tolist())
        database.commit()
    
    
    for key,value in lineMap.iterrows():
        with database.cursor() as cursor:
            cursor.execute("INSERT INTO line_modification(line_key,since) SELECT id,NULL FROM line_registered WHERE line_id=%s AND direction=%s ON CONFLICT DO NOTHING",(value['line'],value['direction']))
            database.commit()
            valuesList = lineData.iloc[range(value['begin'],value['begin']+value['length'])].values.tolist()
            valuesGood = [[i,valuesList[i][0],valuesList[i][1]] for i in range(len(valuesList))]
            cursor.executemany(f"""INSERT INTO line_data(line_version,position,latitude,longitude) VALUES ( 
    (SELECT id 
    FROM line_modification 
    WHERE 
    line_key=(
        SELECT id 
        FROM line_registered 
        WHERE 
            line_id='{value['line']}' AND 
            direction='{value['direction']}'
        ) AND 
    last_change=DATE(NOW()))
    ,%s,%s,%s) ON CONFLICT DO NOTHING""", valuesGood)
        database.commit()

def mainLinesSimple(database,CONFIGS,log):
    error, lineTables = ImportLineData.GatherLineData(CONFIGS,log)
    if error:
        log.critical(error)
        exit()
    
    lineMap,lineData = (lineTables['map'],lineTables['data'])
    with io.StringIO() as buffer:
        for _,val in lineMap.iterrows():
            beginPos = val['begin']
            line = val['line']
            direction = val['direction']
            length = val['length']
            for currentPos in list(range(length)):
                buffer.write(f"""{line},{direction},{currentPos},{lineData.iloc[beginPos+currentPos]['lat']},{lineData.iloc[beginPos+currentPos]['lon']}\n""")
        buffer.seek(0)
    
        with database.cursor() as cursor:
            cursor.copy_from(buffer,'line_data_simple',sep=',')

        database.commit()

def mainLineOld(database, CONFIGS, logger):
    import pickle
    files = os.listdir(CONFIGS['paths']['oldLineImport'])
    with io.StringIO() as buffer:
        for lineFile in files:
            with open(CONFIGS['paths']['oldLineImport']+lineFile,'rb') as pick:
                lineData = pickle.load(pick)
            
            for lineIndex in range(len(lineData['Coordinates'])):
                buffer.write(f"""{lineData['line']},{(1 if lineData['direction'] == "Reverse" else 0)},{lineIndex},{lineData['Coordinates'][lineIndex][1]},{lineData['Coordinates'][lineIndex][0]}\n""")

        buffer.seek(0)
        with database.cursor() as cursor:
            cursor.copy_from(buffer,'line_data_simple',sep=',')
        database.commit()

def mainGetLines(CONFIGS,logger):
    ImportLineData.GetLineDataFromAPI(CONFIGS,logger)

if __name__ == "__main__":

    # -------------------------------------------------------------
    # Setup
    # -------------------------------------------------------------
    

    # Defining configs
    #CONFIGS = ConfigurationScript.createConfig('.')
    CONFIGS = ConfigParser()
    CONFIGS['paths']['importBusPath'] = "/home/fdias/m2/"
    CONFIGS['paths']['importLinePath'] = "/home/fdias/m2/"
    CONFIGS['paths']['oldLineImport'] = 'var/data/lines/data/'

    # Establishing database connection
    database = db.connect(**CONFIGS['database'])


    # ----------------------------------------
    # Logger definition
    # ---------------------------------------

    logger = logging.getLogger(sys.argv[0])
    logger.setLevel(logging.DEBUG)
    
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    fi = logging.FileHandler("PopulateDatabase.log")
    
    fi.setLevel(logging.DEBUG)
    fi.setFormatter(formatter)

    # TODO: CLI Interface
    
    # --------------------------------------------------------------------
    # Data insertion type selection
    # --------------------------------------------------------------------

    # To import bus data from the .tar.gz daily files
    if sys.argv[1] == 'buses':
        days = sys.stdin.read().split('\n')
        daysDate = [datetime.datetime.strptime(i,'%Y-%m-%d') for i in days if len(i) > 0]
        logger.debug(f"Amount of dates: {len(days)}")
        mainBus(days,database,CONFIGS,logger)

    # To import the lines files from the all-lines.hd5 file 
    elif sys.argv[1] == 'lines':
        mainLines(database,CONFIGS,logger)

    # Same as above, but simpler
    elif sys.argv[1] == 'linesSimple':
        mainLinesSimple(database,CONFIGS,logger)

    # Get lines from API and saves to .md5 files
    elif sys.argv[1] == 'getLines':
        mainGetLines(CONFIGS,logger)

    # No clue
    elif sys.argv[1] == 'LinesOld':
        mainLineOld(database,CONFIGS,logger)
    
    print("no method selected")
    exit(0)