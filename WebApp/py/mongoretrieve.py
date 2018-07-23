import pymongo
from pymongo import MongoClient

def mongoRetrieve():
	client = MongoClient()
	db = client.steamusers
	collection = db.userSession
	csv = 'user,connectionTime,gameid,loccountrycode,locstatecode,loccityid,sessionDate\n'
	line = ''
	for obj in collection.find():
		date = obj['_id']
		for user in obj['users']:
			for session in user['sessions']:
				line += session['steamid']+","+str(session['connectionTime'])
				if 'gameid' in session:
					line += ","+session['gameid']
				else:
					line += ",None"
				if 'loccountrycode' in session:
					line += ","+session['loccountrycode']
				else:
					line += ",Unknown"
				if 'locstatecode' in session:
					line += ","+session['locstatecode']
				else:
					line += ",Unknown"
				if 'loccityid' in session:
					line += ","+str(session['loccityid'])
				else:
					line += ",Unknown"

			csv += line+","+date+"\n"
			line = ''
	print("CSV Successfully generated!")

	f = open('./mongoretrieve.csv',"w")
	f.write(csv)           

