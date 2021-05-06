import os
import pandas as pd

import requests
import logging
import pickle
import time
import json
import sys

def FormLineDataSubset(lineMap):
	subset = []
	for key, value in lineMap.iteritems():
		subset += list(range(value['begin'],value['begin']+value['length']))
	return subset
	
# GatherLineData(CONFIGS,lineSelection=None)
# Arguments:
# 	CONFIGS - Default configuration object from main.GI_ConfigurationScript
#	lineSelection (Optional, Default: None) - List of strings indicating desired lines for data filtering. If not defined, all lines are returned
# Returns:
# 	(lineMap,lineData)
#	lineMap - Pandas' DataFrame wich associates fragments of lineData with respective lines
#	lineData - Dataframe containing coordinates for all lines, separation in lineMap
#
def GatherLineData(CONFIGS,log,lineSelection = None):
	lineFileName = CONFIGS['paths']['importLinePath'] + 'all_lines.hd5'

	if not os.path.isfile(lineFileName):
		return f"Line data file does not exist",0

	lineTables = LoadData(lineFileName)
	if lineSelection:
		lineTables['map'] = lineTables['map'].loc[lineTables['map']['line'].isin(lineSelection)]
		dataIndexFilter = []
		for _, entry in lineTables['map'].iterrows():
			dataIndexFilter += list(range(int(entry['begin']),int(entry['begin'])+int(entry['length'])))
		lineTables['data'] = lineTables['data'].iloc[dataIndexFilter]
	return 0,lineTables


# LoadData(filePath)
# Arguments:
# 	filePath - file name and full path
# Returns:
# 	(lineMap,lineData)
# Description:
# 	Get's file name and full path and returns line map and data dataframes.
def LoadData(filePath):
	lineMap, lineData = pd.read_hdf(filePath,"map"), pd.read_hdf(filePath,"data")
	lineTables = {"map":lineMap,'data':lineData}
	return lineTables

# TODO: Implement this Function
# GenerateData()
# Arguments:
# 	Path were to save
# Returns: Error code 0 sucess, ~0 fail
# Description:
# 	Retrieves data from API and saves into all_lines.hdf file.


def GetLineDataFromAPI(CONFIGS,log):
		numberOfArguments = 200

		log.debug("Requesting available line_id")
		error, busLineList = GetAllAvailableBusLinesIds()
		if error > 0:
			print(error)
			return error
		
		log.debug(f"Lines on database: {len(busLineList)}")
		
		result = []
		infoTotalBuses = len(busLineList)
		infoCount = 0

		busLineDivided = []
		for index in list(range(0,len(busLineList),numberOfArguments)):
			busLineDivided.append(busLineList[index:index+numberOfArguments])

		lineMap = pd.DataFrame(columns=["line",'begin','length','operator','direction','description'])
		lineData = pd.DataFrame(columns=['lat','lon'])
			
		for idLists in busLineDivided:

			objectIds = ""
			for item in idLists:
				objectIds += f"{item},"

			parameters = {
				"objectIds":objectIds[:-1],
				"outFields":"Id_Rota,Nome_Rota,direction_id,shape_id,Linhas,Operadora,SHAPE",
				"f":"json"
			}
			log.debug(f'requesting {numberOfArguments} Ids from API')
			error, request = MakeAPIRequest(parameters)
			
			if error:
				print(error)
				return error
			
			log.debug('Request successfull')
			
			currentRow = 0
			
			for busLinePath in request["features"]:
				print(busLinePath['attributes']['Id_Rota'])
				infoCount += 1
				result = {}
				result["line"] = busLinePath["attributes"]["Id_Rota"]
				result["operator"] = busLinePath["attributes"]["Operadora"]
				result["direction"] = busLinePath["attributes"]["direction_id"]
				result["description"] = busLinePath["attributes"]["Nome_Rota"]
				busPositions = busLinePath["geometry"]["paths"][0]
				result['begin'] = currentRow
				result['length'] = len(busPositions)
				
				lineMapRow = pd.Series(result)
				currentRow += len(busPositions)
				lineDataAppend = pd.DataFrame(busPositions,columns=['lon','lat'])
				lineMap = lineMap.append(lineMapRow,ignore_index = True)
				lineData = lineData.append(lineDataAppend,ignore_index =True)

		filename = CONFIGS['paths']['importLinePath'] + "all_lines.hd5"
		store = pd.HDFStore(filename)
		store['map'] = lineMap
		store['data'] = lineData
		store.close()
		return 0




def MakeAPIRequest(parameters):

	#print("Initiating request")
	#print("1-sec sleep interval")
	time.sleep(1)
	#print("Iniciating request")
	# Makes a request that receives all available lines in query
	response = requests.get("https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Transporte_Trafego/GTFS/MapServer/0/query",params=parameters)


	if (response.status_code != 200):
		print(f"API request not successful (response code: {response.status_code})")
		return 1, []

	#print("Request successful")
	return (0,  response.json())


def GetAllAvailableBusLinesIds():

	# Parameters for sending request in one direction
	parameters = {
		"where":"Id_Rota IS NOT NULL",
		"outFields":"Id_Rota",
		"f":"json",
		"returnIdsOnly":"true"
	}

	error, result = MakeAPIRequest(parameters)

	if error:
		return (2, {})
	busLineList = result["objectIds"]

	return 0, busLineList


