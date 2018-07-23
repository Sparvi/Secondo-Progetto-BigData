import sys
import requests
import json

if __name__ == "__main__":

	# Defining params
	api_url = 'http://api.steampowered.com/'
	friends_interface_name = 'ISteamUser/'
	getFriendList = 'GetFriendList/'
	v1 = 'v0001/'
	steam_session_key= '?key=998D8F85D3E92EAA590EE41DDDFFE54B'
	steam_user_id = '&steamid=76561198024149147'
	relationship = '&relationship=friend'
	data_format = '&format=xml'
	app_id = '578080'
	ids_list = []
	visited = []
	raw_ids_list = set(ids_list)
	accepted_ids = set(ids_list)
	pending_ids = set(ids_list)
	print("Setting up the request...")
	http_request = api_url+friends_interface_name+getFriendList+v1+steam_session_key+steam_user_id+relationship
	print("Sending Packets...")
	r = requests.get(http_request)
	print("Sending http request to: %s" % r.url)
	print()
	dataset = r.json()['friendslist']['friends']
	i = 0
	for field in dataset:
		raw_ids_list.add(dataset[i]['steamid'])
		i += 1
	print("Caching friendlist for %s" %steam_user_id)

	# Defining gamelist http request params
	games_interface_name = 'IPlayerService/'
	owned_games = 'GetOwnedGames/'
	print("Requesting owned games for user")
	while(len(accepted_ids) < 3000):
		for current_id in raw_ids_list:
			if(len(accepted_ids) >= 3000):
				break
			if current_id not in visited:
				visited.append(current_id)
				http_request = api_url+games_interface_name+owned_games+v1+steam_session_key+"&steamid="+current_id
				r = requests.get(http_request)
				try:
					gamelist = r.json()['response']['games']
					i = 0
					print("Getting Gamelist for id: %s" %current_id)
					for game in gamelist:
						found_id = str(gamelist[i]['appid'])
						if( found_id == app_id or found_id == '570' or found_id == '730'):
							print("MATCH FOUND - YEEH AAH!!!")
							accepted_ids.add(current_id)
							print("ACTUAL SET SIZE: "+str(len(accepted_ids)))
							pending_ids.add(current_id)
						i += 1
				except KeyError:
					print(current_id+" has no gamelist/no public profile!")
			else:
				print("Profile alredy visited!")
		print("%s profiles visited!" %len(visited))
		if(len(accepted_ids) >= 3000):
			break
		else:
			raw_ids_list = set(ids_list)
			new_id = pending_ids.pop()
			print("Fetching from another branch...")
			http_request = api_url+friends_interface_name+getFriendList+v1+steam_session_key+"&steamid="+new_id+relationship
			r = requests.get(http_request)
			try:
				dataset = r.json()['friendslist']['friends']
				j = 0
				for field in dataset:
					raw_ids_list.add(dataset[j]['steamid'])
					j += 1
			except KeyError:
				print(new_id+" has no friendslist!")
	print("computation complete, writing file!")
	print(len(accepted_ids))
	f = open('./ids.txt', 'w+')
	for elem in accepted_ids:
		f.write(str(elem)+"\n")
	f.close()
	print("ALL DONE!")