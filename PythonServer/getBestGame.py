from __future__ import print_function
import sys
import datetime
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pymongo
from pymongo import MongoClient
import traceback


def saveToMongo(data):
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client.steamusers
        collection = db.gameList
        transaction = {}
        transaction = data.first()
        transaction['_id'] = 'gamelist'
        record_id = collection.replace_one({'_id':transaction['_id']}, transaction, True)
        print("Data pushed!")
    except (TypeError,ValueError):
        print("Waiting for Data!")
        #print(traceback.format_exc())

def retrieveGame(x):
    if 'gameid' in x:
        return (x['gameid'],1)
    else:
        return ("offline",1)

def updateGamesCount(currentCount, countState):
    
    if not currentCount:
        return countState
    else:
        x = currentCount[-1]

        if 'gameid' in x:
            gameid = x['gameid']
        else:
            gameid = "No Game"
        
        countState = gameid
        
        return countState

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Best-Games")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
  
    ssc.checkpoint("/tmp")

    filterx = kvs.map(lambda v: json.loads(v[1])) \
                    .flatMap(lambda player: (player['response']['players'])) \
                    .map(lambda x: (x['steamid'],x))\
                    .updateStateByKey(updateGamesCount)\
                    .map(lambda x: (x[1],1))\
                    .reduceByKey(lambda a, b: a + b)\
                    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))\
                    .map(lambda x: {'appid':x[0], 'count':x[1]})\
                    .map(lambda x: (1, [x])) \
                    .reduceByKey(lambda x, y: x+y) \
                    .map(lambda x:{'games':x[1]}) \
                    .foreachRDD(saveToMongo)
    ssc.start()
    ssc.awaitTermination()
