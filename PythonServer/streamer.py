import sys
import requests
import json
import time
from kafka import KafkaProducer

if __name__ == "__main__":

	init = True;

	# Setting up Kafka connection params
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

	# Topic Creation
	topic = 'topic'
	currentDate = time.struct_time(time.gmtime(0))


	# Defining params
	window_time = 2.0
	api_url = 'http://api.steampowered.com/'
	iSteamUser = 'ISteamUser/'
	getPlayerSummaries = 'GetPlayerSummaries/'
	version = 'v0002/'
	key = '?key="inserisci qui la chiave"'


	f = open('./ids.txt', 'r')
	players = f.read().splitlines()
	playerList = ''
	print("Setting up players list...")
	i = 0
	mapping = {}

	playerJsonList = []

	for player in players:
		i += 1
		if i%100 == 0:
			playerList += player
			mapping[i] = playerList[1:]
			playerList = ''
		else:
			playerList = playerList+","+player
		playerJsonList.append({"steamid":player,"personastate":999})

	pill = {"response":{"players":playerJsonList}}
	print(pill)

	while True:
		print("########### STARTING NEW LOOP ########### ")
		for value in mapping.values():
			ts = time.struct_time(time.gmtime(time.time()))
			if(ts[4]-currentDate[4]>=2 and init is False):
				currentDate = ts
				print("Sending pill")
				#producer.send(topic,pill)
			print("Sending Request!")
			http_request = api_url+iSteamUser+getPlayerSummaries+version+key+'&steamids='+str(value)
			r = requests.get(http_request)
			producer.send('topic', r.json())
			init = False
			window = time.struct_time(time.gmtime(time.time()))[5]-ts[5]
			try:
				time.sleep(window_time-window)
			except:
				print("DELAY!")