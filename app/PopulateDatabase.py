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
import argparse
import pathlib
from configparser import ConfigParser


import psycopg2 as db

# from .main import ConfigurationScript
import ImportBusData
import ImportLineData


def mainBus(args,database,CONFIGS,log):
    desiredDate = args.date
    log.info(f"Selected mode: bus")
    log.info(f"Inserting data from {desiredDate}")
    log.info(f"Gathering from file")

    _, busTable = ImportBusData.GatherBusData(desiredDate,CONFIGS,logging)
    
    log.info("Converting to CSV")
    with io.StringIO() as buffer:
        buffer.write(busTable[['time','id','lat','lon','line']].to_csv(na_rep='\\N',index=False,header=False,line_terminator=",\n"))
        buffer.seek(0)
        log.info("Sending to database")
        with database.cursor() as cursor:
            cursor.copy_from(buffer,'bus_insertion',sep=',')
            cursor.execute("SELECT COUNT(*) FROM bus_data")
            results = cursor.fetchall()
            log.debug(f"written {results[0][0]} new entries")
            database.commit()
    
def mainLines(args,database,CONFIGS,log):
    error, lineTables = ImportLineData.GatherLineData(CONFIGS,log)
    log.info("Lines imported")
    if error:
        log.critical(error)
        exit()
    
    lineMap,lineData = (lineTables['map'],lineTables['data'])
    with io.StringIO() as buffer:
        log.info("Writing to buffer")
        for _,val in lineMap.iterrows():
            beginPos = val['begin']
            line = val['line']
            direction = val['direction']
            length = val['length']
            for currentPos in list(range(length)):
                buffer.write(f"""{line},{direction},{currentPos},{lineData.iloc[beginPos+currentPos]['lat']},{lineData.iloc[beginPos+currentPos]['lon']}\n""")
        buffer.seek(0)
        log.info("Sending to database")
        with database.cursor() as cursor:
            cursor.copy_from(buffer,'line_data',sep=',')

        database.commit()
    log.info("Done")

def mainGetLines(args,database,CONFIGS,logger):
    ImportLineData.GetLineDataFromAPI(CONFIGS,logger)

if __name__ == "__main__":

    # References to different functions based on desired mode
    ImplementedModes = {
        'bus':mainBus,
        'lines':mainLines,
        'get_lines':mainGetLines
    }

    FilesDir = pathlib.Path("/run/secrets")
    DefaultConfigurationsFile = FilesDir/ "main_configurations"
    DatabaseCredentialsFile = FilesDir/ "db_credentials"
    # -------------------------------------------------------------
    # Setup
    # -------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Script to write data to database")
    parser.add_argument("mode",type=str,choices=ImplementedModes.keys(),help='Mode selection (bus/line)')
    parser.add_argument('-c',"--config",default=DefaultConfigurationsFile,type=pathlib.Path,help="Configuration file full path")
    parser.add_argument('-d',"--date",type=datetime.date.fromisoformat,default=None,help="Desired date for single mode data insertion (YYYY-MM-DD)")
    parser.add_argument('-m',"--multi",nargs="+",help="Execute same command for multiple dates/files given as option values")
    parser.add_argument("-v",'--verbose',action="count",default=0,help="Increase output verbosity")
    parser.add_argument("-b","--bus",default=None,help='bus data insertion path')
    parser.add_argument("-l","--line",default=None,help='bus data insertion path')
    args = parser.parse_args()

    # ----------------------------------------
    # Logger definition
    # ---------------------------------------

    logger = logging.getLogger(sys.argv[0])
    logger.setLevel(logging.DEBUG)
    
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    
    logLevel = logging.ERROR * (args.verbose == 0) + \
                logging.WARNING * (args.verbose == 1) + \
                logging.INFO * (args.verbose == 2) + \
                logging.DEBUG * (args.verbose >= 3)

    ch.setLevel(logLevel)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    
    # Defining configs
    CONFIGS = ConfigParser()
    CONFIGS.add_section('paths')
    CONFIGS['paths']['importBusPath'] = '/collect_bus_old/' if args.bus is None else args.bus
    CONFIGS['paths']['importLinePath'] = '/collect_line/' if args.line is None else args.line
    logger.debug(f"Reading configs from {args.config}")
    CONFIGS.read(args.config)
    if DatabaseCredentialsFile:
        logger.debug(f"Reading database credentials from {DatabaseCredentialsFile}")
        CONFIGS.read(DatabaseCredentialsFile)
    
    # --------------------------------------------------------------------
    # Data insertion type selection
    # --------------------------------------------------------------------
    logger.debug(f"""Parameters: mode '{args.mode}'/ verbose: '{args.verbose}'/ config '{args.config}' / date '{args.date}' """)
    logger.debug("Database connection attempt")
    logger.debug(f"database credentials:")
    logger.debug(f"\tusername: '{CONFIGS['database']['user']}'")
    logger.debug(f"\tpassword: 'nice try'")
    logger.debug(f"\tusername: '{CONFIGS['database']['host']}'")
    logger.debug(f"\tusername: '{CONFIGS['database']['database']}'")
    
    # Establishing database connection
    database = db.connect(**CONFIGS['database'])

    if args.mode == "bus" and args.date is None:
        logger.critical("Date not specified for bus mode. Can't proceed to save file. Exiting...")
        exit(1)

    ImplementedModes[args.mode](args,database,CONFIGS,logger)

    logger.info("Insertion complete.")
    exit(0)