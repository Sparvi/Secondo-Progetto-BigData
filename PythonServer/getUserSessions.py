from __future__ import print_function

import sys
import pymongo
import traceback
import pprint
import time
import datetime
from pymongo import MongoClient
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def saveToMongo(data):
	try:
		client = MongoClient('mongodb://localhost:27017/')
		db = client.steamusers
		collection = db.userSession
		transaction_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
		transaction = {}
		transaction = data.first()
		transaction['_id'] = transaction_time
		record_id = collection.replace_one({'_id':transaction['_id']}, transaction, True)
		print("Data saved!")
	except (TypeError, ValueError):
		print("Waiting for Data...")

def updatePlayerCount(currentCount, countState):
	for ccjson in currentCount:
		check = False
		if countState is None:
			countState = []
			csjson = ccjson
			if ccjson['personastate'] == 999:
				csjson['personastate'] = 0
			csjson['connectionTime'] = 0
			countState.append(csjson)
			return countState
		else:
			if ccjson['personastate'] == 999:
				print("\n\n\nEcco La Pill\n\n\n")
				ccjson['personastate'] = 0
				for csjson in countState:
					csjson['personastate'] = 0
					csjson['connectionTime'] = 0
			else:
				if  ccjson['personastate'] != 0:
					for csjson in countState:
						if ccjson['personastate'] == 999:
							print("\n\n\nEcco La Pill\n\n\n")
							ccjson['personastate'] = 0
							for csjson in countState:
								csjson['personastate'] = 0
								csjson['connectionTime'] = 0
							return countState
						csjson['personastate'] = ccjson['personastate']
						if 'gameid' in ccjson and 'gameid' in csjson and ccjson['gameid'] == csjson['gameid']:
							check = True
							csjson['connectionTime']=(csjson['connectionTime']+1)
							break
						elif 'gameid' not in ccjson and 'gameid' not in csjson:
							check = True
							csjson['connectionTime']=(csjson['connectionTime']+1)
					if(check is False):
						ccjson['connectionTime']=0
						countState.append(ccjson)
				else:
					for csjson in countState:
						csjson['personastate'] = ccjson['personastate']
						if 'gameid' in ccjson and 'gameid' in csjson and ccjson['gameid'] == csjson['gameid']:
							check = True
							csjson['connectionTime']=(csjson['connectionTime'])
							break
						elif 'gameid' not in ccjson and 'gameid' not in csjson:
							check = True
							csjson['connectionTime']=(csjson['connectionTime'])
					if(check is False):
						ccjson['connectionTime']=0
						countState.append(ccjson)
	return countState

def clean(rdd):
	if 'avatarmedium' in rdd:
		del rdd['avatarmedium']
	if 'avatar' in rdd:
		del rdd['avatar']
	if 'avatarfull' in rdd:
		del rdd['avatarfull']
	if 'profileurl' in rdd:
		del rdd['profileurl']
	return rdd

if __name__ == "__main__":
	print("Creating Spark Context")
	sc = SparkContext(appName="getUserLocation")  
	sc.setLogLevel("ERROR")

	# Creating Streaming Context
	ssc = StreamingContext(sc, 1)
	ssc.checkpoint("./tmp")

	# Connect context to kafka
	print("Connecting Spark Context to Kafka")
	kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'org.apache.start', {'topic':1})

	filterx = kvs.map(lambda v: json.loads(v[1])) \
					.flatMap(lambda player: (player['response']['players'])) \
					.map(lambda x: (x['steamid'],clean(x)))

	totalCounts = filterx.updateStateByKey(updatePlayerCount) \
							.map(lambda x:{'user':x[0],'sessions':x[1]}) \
							.map(lambda x: (1, [x])) \
							.reduceByKey(lambda x, y: x+y) \
							.map(lambda x:{'users':x[1]}) \
							.foreachRDD(saveToMongo)

	ssc.start()
	ssc.awaitTermination()