
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo
from pymongo import MongoClient
import traceback

def saveCountriesToMongo(data):
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client.steamusers
        collection = db.userCountries
        transaction = {}
        transaction = data.first()
        transaction['_id'] = 'countries'
        record_id = collection.replace_one({'_id':transaction['_id']}, transaction, True)
        print("Data pushed!")
    except (TypeError,ValueError):
        print("Waiting for Data!")
        #print(traceback.format_exc())

def saveDetailedToMongo(data):
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client.steamusers
        collection = db.userCountriesDetailed
        transaction = {}
        transaction = data.first()
        transaction['_id'] = 'countries'
        record_id = collection.replace_one({'_id':transaction['_id']}, transaction, True)
        print("Data pushed!")
    except (TypeError,ValueError):
        print("Waiting for Data!")
        #print(traceback.format_exc())

def updatePlayerCount(currentCount, countState):

    if not currentCount:
        return countState
    else:
        x = currentCount[-1]

        if 'loccountrycode' in x:
            loccountrycode = x['loccountrycode']
        else:
            loccountrycode = "Unknown"
        if 'locstatecode' in x:
            locstatecode = x['locstatecode']
        else:
            locstatecode = "Unknown"
        if 'loccityid' in x:
            loccityid = x['loccityid']
        else:
            loccityid = "Unknown"
        if 'personastate' in x:
            personastate = x['personastate']
        else:
            personastate = "0"
        
        countState = (loccountrycode,locstatecode,loccityid,personastate)
        
        return countState

def filterOnline(player):
    if player[3] is 0:
        return ((player[0],player[1],player[2]),0)
    else:
        return ((player[0],player[1],player[2]),1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
  
    ssc.checkpoint("/tmp")

    filterx = kvs.map(lambda v: json.loads(v[1])) \
                    .flatMap(lambda player: (player['response']['players'])) \
                    .map(lambda x: (x['steamid'],x))\
                    .updateStateByKey(updatePlayerCount)\
                    .map(lambda x: filterOnline(x[1]))\
                    .reduceByKey(lambda a, b: a + b) \
                    .map(lambda x:{'country':x[0][0], 'connections':x[1], 'details': {'state':x[0][1], 'city':x[0][2]}})

    countries = filterx .map(lambda x:(x['country'], x['connections'])) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x: {'country':x[0], 'connections':x[1]}) \
                        .map(lambda x: (1, [x])) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x: {'locations':x[1]})

    detailed = filterx  .map(lambda x: (1, [x])) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x: {'cities':x[1]})

    countries.foreachRDD(saveCountriesToMongo)
    detailed.foreachRDD(saveDetailedToMongo)

    ssc.start()
    ssc.awaitTermination()
