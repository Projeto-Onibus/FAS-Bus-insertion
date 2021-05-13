#!/usr/bin/python3.7
#
#	ImportData.py
#	Fernando Dias (fernandodias@gta.ufrj.br)
#
#	Description: Gathers raw data taken from the servers and orders it for further analysis
#
#

import re
import pickle
import tarfile
import datetime
import os

import pandas as pd
import numpy as np


# For later testing
test_totalElements = 0
test_filteredElements = 0


def SaveData(data, date, fileName):
	data.to_hdf(fileName, "data")
	return None

def OpenSavedData(date, fileName):
	processedData = pd.read_hdf(fileName, "data")
	return None, processedData

def FilterData(data,date,debug=""):
	dataPattern = re.compile(r"^\d\d-\d\d-\d\d\d\d \d\d:\d\d:\d\d$")
	dataIso = re.compile(r"\d\d-\d\d-\d\d\d\d")
	validDate = False
	dataOk = True

	if dataIso.search(date):
		validDate = True
		dateFormat = date[5:7]+"-"+date[8:]+"-"+date[:4]

	# See if dates matches a date pattern
	if not dataPattern.search(data[0]):
		return not dataOk

	# See if data matches number of columns
	if len(data) != 6:
		return not dataOk

	# See if the current date matches entry
	if validDate:
		if data[0][:10] != dateFormat:
			return not dataOk

	# See if coordinates were given
	if len(data[3]) < 2 or len(data[4]) < 2:
		return not dataOk

	# See if coords are "floatabble" lol
	try:
		float(data[3][1:-1])
		float(data[4][1:-1])
	except ValueError:
		return not dataOk

	# See if position isn't 0 or positive (not in brazil)
	if float(data[3][1:-1]) > -1 or float(data[4][1:-1]) > -1:
		return not dataOk

	# See if identification is not null or too long
	if len(data[1]) == 0 or len(data[1]) > 10:
		return not dataOk

	return dataOk

def GatherBusData(desiredDate,CONFIGS,log,busSelection=None):
	"""
def GatherBusData(desiredDate,Configs,busSelection=None)
Arguments:
	desiredDate (str) - Data no padrao ISO YYYY-MM-DD ou desiredDate
	CONFIGS (obj) - Objeto de configuracao do script
	busSelection (list) - Lista de onibus para filtrar nos dados (Default:None, nao faz filtragem)
Returns:
	errorCode - 0 if success or str describing error
	busData - DataFrame containing bus data
	"""
	#dateFormat = re.compile(r"^\d\d\d\d-\d\d-\d\d$")
	#if type(desiredDate) is str and not dateFormat.match(desiredDate):
	#	return "Date does not match iso standard YYYY-MM-DD", None

	if type(desiredDate) is datetime.datetime or type(desiredDate) is datetime.date:
		desiredDate = desiredDate.isoformat()
	

	# Formatando os dados da configs para uso na funcao
	dataPath = CONFIGS['paths']['importBusPath']
	busRawFileName = dataPath + desiredDate + ".tar.gz"
	busProcessedFileName = dataPath + desiredDate + ".hd5"


	# Verifica se os arquivos existem e sai da rotina caso contrario
	processedFile = False
	if not os.path.isfile(busProcessedFileName):
		if not os.path.isfile(busRawFileName):
			log.error(f"Bus data '{busProcessedFileName}' or '{busRawFileName}' does not exist")
			return f"Bus data '{busProcessedFileName}' or '{busRawFileName}' does not exist",None
		else:
			processedFile = False
	else:
		processedFile = True
	
	# Importando os dados para o codigo
	if processedFile:
		error, busDataRaw = OpenSavedData(desiredDate,busProcessedFileName)
		if error:
			return error, None
	else:
		error, busDataRaw = ProcessRawData(busRawFileName,desiredDate)
		if error:
			return error, None
		
		SaveData(busDataRaw,desiredDate,busProcessedFileName)
		#if error:
		#	return error, None

	
	return processedFile, busDataRaw

def ProcessRawData(filename,desiredDate):
	limitEntries = False
	test_totalElements = 20
	test_filteredElements = 32

	rawDataList = []

	currentDirectory = tarfile.open(filename)
	filesToExtract = currentDirectory.getmembers()
	try:
		for currentFile in filesToExtract:
			if test_totalElements == 30 and limitEntries:
				break
			if not currentFile.isfile():
				continue
			CurrentInput = currentDirectory.extractfile(currentFile)
			while 1:
				if test_totalElements == 30 and limitEntries:
					break
				line = CurrentInput.readline()
				if not line:
					break
				if len(line) < 2:
					continue
				rawText = line.decode("UTF-8")[:-1].split(",")
				for element in rawText:
					if len(element) <= 1:
						continue
					if len(element) > 0 and element[0] == '"':
						element = element[1:]
					if len(element) > 0 and element[-1] == "\"":
						element = element[:-1]
				if FilterData(rawText,desiredDate):
					time = datetime.datetime.strptime(rawText[0],"%m-%d-%Y %H:%M:%S")

					rawDataList.append([time]+[rawText[1]]+[rawText[2][:7] if len(rawText[2])>0 else None] +[float(rawText[3][1:-1]),float(rawText[4][1:-1]),float(rawText[5])])

				# For testing purposes
				else:
					test_filteredElements += 1
				test_totalElements += 1

	except Exception as err:
		print(f"Erro ({err}) na linha ({line}) com o elemento ({element})")

	rawDataFrame = pd.DataFrame(rawDataList,
		columns=["time","id","line","lat","lon","speed"]
	).drop_duplicates()


	return 0, rawDataFrame


