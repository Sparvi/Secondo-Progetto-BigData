import sys
import time
import json
import pymongo
import datetime
import pprint
from pymongo import MongoClient
from mongoretrieve import mongoRetrieve

if __name__ == "__main__":
	client = MongoClient('mongodb://localhost:27017/')
	db = client.steamusers
	countries = db.userCountries
	games = db.gameList
	with open('./applist.json') as apps:
		appjson = json.load(apps)
	init = True
	while True:
		action_time = datetime.datetime.fromtimestamp(time.time()).strftime('%H-%M-%S')
		connections = 0
		output='{'
		result = countries.find()
		for current in result:
			for l in current['locations']:
				n_o_connections = l['connections']
				connections += n_o_connections
				output += '"'+l['country']+'": '+str(n_o_connections)+','
			output = output[:-1]
		output += "}"
		print("Total Connections retrieved:\t %s ONLINE" % connections)
		connections = 0
		f = open('./connections.json', 'w')
		f.write(output)
		f.close()

		output = 'Game Id, Game Name, Players Online\n'

		result = games.find()
		for current in result:
			for game in current['games']:
				realname = ''
				n_o_counts = game['count']
				appid = str(game['appid'])
				connections += n_o_counts
				for this in appjson['applist']['apps']:
					if(appid == str(this['appid'])):
						realname = this['name']
						break
					elif appid == 'No Game':
						realname = 'Steam Home'
				output += '"'+appid+'", "'+str(realname.encode('utf-8'))+'", '+str(n_o_counts)+'\n'
		print("Total connections for gamelist:\t%s" % connections)
		connections = 0
		f = open('./games.csv',"w")
		f.write(output)    
		if(action_time == '00-00-00' or init is True):
			init = False
			mongoRetrieve()
		print("Data retrieved succesfully!")
		time.sleep(5)