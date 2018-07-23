from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import sys
import time
import glob, re, os
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import numpy as np
import pprint
import pandas as pd
from pandas.plotting import table

if __name__ == "__main__":
	if(len(sys.argv) != 4):
		print("please, specify input file path: bin/spark-submit path/to/file.py path/to/inputfile startDate endDate");
		exit(-1);


	spark = SparkSession \
	    .builder \
	    .appName("Python Spark SQL") \
	    .getOrCreate()

	df = spark.read.load(sys.argv[1], format="csv", sep=",", inferSchema="true", header="true")
	df.registerTempTable("t1")


	df = spark.sql("SELECT * FROM t1 ORDER BY user")
	df.show()

	startDate = sys.argv[2]
	endDate = sys.argv[3]



	#======== QUERY 1 ========#

	query1="SELECT gameid, COUNT(DISTINCT(user)) as numberOfPlayer, AVG(connectionTime) as AverageTime, STDDEV(connectionTime) as STDDEV FROM t1 WHERE sessionDate>=\"{0}\" AND sessionDate<=\"{1}\" GROUP BY gameid ORDER BY numberOfPlayer DESC".format(startDate,endDate)
	df_SELECT = spark.sql(query1)
	df_SELECT.show()
	df_SELECT.coalesce(1).write.mode("overwrite").csv("./bestGame",header=True)
	for filename in glob.glob('./bestGame/*.csv'):
	    os.rename(filename, "./bestGame/bestGame.csv")
	df_SELECT.registerTempTable("gamesCount")

	#======== PLOT 1 ========#
	df_SELECT = spark.sql("SELECT * FROM gamesCount WHERE gameid <> 'None'")
	pandas_df = df_SELECT.toPandas()
	ax1 = pandas_df.plot.bar(x='gameid', y='numberOfPlayer', rot=90, fontsize=5)
	plt.savefig('gamesCount.png')

	#======== QUERY 1 ========#

	query2="SELECT loccountrycode, locstatecode, loccityid, COUNT(DISTINCT(user)) as numberOfPlayer, AVG(connectionTime) as AverageTime, STDDEV(connectionTime) as STDDEV FROM t1 WHERE sessionDate>=\"{0}\" AND sessionDate<=\"{1}\" AND connectionTime>0 GROUP BY loccountrycode, locstatecode, loccityid ORDER BY numberOfPlayer DESC".format(startDate,endDate)
	df_SELECT = spark.sql(query2)
	df_SELECT.show()
	df_SELECT.coalesce(1).write.mode("overwrite").csv("./player2city",header=True)
	for filename in glob.glob('./player2city/*.csv'):
	    os.rename(filename, "./player2city/player2city.csv")



	#======== QUERY 1 ========#

	query3="SELECT loccountrycode, locstatecode, COUNT(DISTINCT(user)) as numberOfPlayer, AVG(connectionTime) as AverageTime, STDDEV(connectionTime) as STDDEV FROM t1 WHERE sessionDate>=\"{0}\" AND sessionDate<=\"{1}\" AND connectionTime>0 GROUP BY loccountrycode, locstatecode  ORDER BY numberOfPlayer DESC".format(startDate,endDate)
	df_SELECT = spark.sql(query3)
	df_SELECT.show()
	df_SELECT.coalesce(1).write.mode("overwrite").csv("./player2state",header=True)
	for filename in glob.glob('./player2state/*.csv'):
	    os.rename(filename, "./player2state/player2state.csv")

	#======== QUERY 1 ========#
	query4="SELECT loccountrycode, COUNT(DISTINCT(user)) as numberOfPlayer, AVG(connectionTime) as AverageTime, STDDEV(connectionTime) as STDDEV FROM t1 WHERE sessionDate>=\"{0}\" AND sessionDate<=\"{1}\" AND connectionTime>0 GROUP BY loccountrycode ORDER BY numberOfPlayer DESC".format(startDate,endDate)
	df_SELECT = spark.sql(query4)
	df_SELECT.show()
	df_SELECT.coalesce(1).write.mode("overwrite").csv("./player2country",header=True)
	for filename in glob.glob('./player2country/*.csv'):
	    os.rename(filename, "./player2country/player2country.csv")
	df_SELECT.registerTempTable("countryCount")
	#======== PLOT 1 ========#
	df_SELECT = spark.sql("SELECT * FROM countryCount WHERE loccountrycode <> 'Unknown'")
	pandas_df = df_SELECT.toPandas()
	ax1 = pandas_df.plot.bar(x='loccountrycode', y='numberOfPlayer', rot=90, fontsize=5)
	plt.savefig('countryCount.png')

	#======== QUERY 1 ========#
	query5="SELECT user, CAST(gameid AS BigInt) from t1 WHERE gameid<>'None'"
	df_SELECT = spark.sql(query5)
	df_SELECT.registerTempTable("t2")
	query5="SELECT tab1.user, tab1.gameid as game1,tab2.gameid as game2 FROM t2 as tab1, t2 as tab2 WHERE  AND tab1.sessionDate>=\"{0}\" AND tab1.sessionDate<=\"{1}\" AND tab1.connectionTime>0 AND tab2.sessionDate>=\"{2}\" AND tab2.sessionDate<=\"{3}\" AND tab2.connectionTime>0 AND tab1.user=tab2.user AND tab1.gameid>tab2.gameid".format(startDate,endDate,startDate,endDate)
	df_SELECT = spark.sql(query5)
	df_SELECT.show()
	df_SELECT.registerTempTable("t3")
	pandas_df = df_SELECT.toPandas()
	pandas_df = pandas_df.apply(pd.to_numeric)
	try:
		pandas_df.plot.hexbin(x='game1', y='game2', cmap="Reds",gridsize=100)
		plt.show()
	except (ValueError):
		print("ERRORE: hexbin requires x column to be numeric")
	query6="SELECT game1, game2, COUNT(DISTINCT(user)) FROM t3 WHERE game1<>'None' AND game2<>'None' GROUP BY game1, game2 ORDER BY game1"
	df_SELECT = spark.sql(query6)	
	df_SELECT.show()
	df_SELECT.coalesce(1).write.mode("overwrite").csv("./correlation",header=True)
	for filename in glob.glob('./correlation/*.csv'):
	    os.rename(filename, "./correlation/correlation.csv")
