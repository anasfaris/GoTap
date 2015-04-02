#reference https://github.com/geduldig/twitterapi
#Twitter search API

from TwitterAPI import TwitterAPI

def get_twitter_result(search):

	CONSUMER_KEY = 'SECRET'
	CONSUMER_SECRET = 'SECRET'
	ACCESS_TOKEN_KEY = 'SECRET'
	ACCESS_TOKEN_SECRET = 'SECRET'

	api = TwitterAPI(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
	
	search_pattern = [search.replace(" ",""), search, search.split()[0], search[1:], '@' + search.replace(" ","")[1:]]
	twitter_result = []
	MAX_TWEET = 3
	count = MAX_TWEET

	try:
		for pattern in search_pattern:
			r = api.request('search/tweets',{'q':pattern, 
											 'count':count, 
											 'result_type':'mixed',
											 'lang':'en'})

			for item in r.get_iterator():
				twitter_result.append([item['user']['screen_name'], 
									   item['user']['profile_image_url'],
									   item['text']])

			if len(twitter_result) > MAX_TWEET-1:
				break
			else: 
				count = MAX_TWEET - len(twitter_result)
	except:
		twitter_result = []

	return twitter_result

	#twitter return http 429 too many request if exceeds quota 180 per 15 minutes

#reference https://github.com/Instagram/python-instagram
#Instagram search API

from instagram.client import InstagramAPI

def get_instagram_result(search):
	search_pattern = [search.replace(" ",""),search.split()[0]]
	instagram_result = []
	MAX_PHOTO = 6
	counts = MAX_PHOTO

	CLIENT_ID = 'SECRET'
	CLIENT_SECRET = 'SECRET'

	api = InstagramAPI(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)


	for pattern in search_pattern:
		recent_media = 0
		try:
			recent_media, mynext = api.tag_recent_media(count=counts, tag_name=pattern)
		except:
			pass

		if recent_media:
			for item in recent_media:
				if item.user.username not in list_banned('instagram'):
					instagram_result.append([item.images['standard_resolution'].url, 
											 item.user.username])
				else:
					print 'some item banned'

		if len(instagram_result) > MAX_PHOTO-1:
			return instagram_result[:MAX_PHOTO]
			break
		else: 
			counts = MAX_PHOTO - len(instagram_result)		

	return instagram_result

def list_banned(platform):
	if platform == 'twitter':
		banned = []
	elif platform == 'instagram':
		banned = ['girlofinsta','shoutoutz4allz93','ellizibethpaige','laaaryb','mintmagazine.co']
	return banned

#reference https://github.com/ZeevG/python-forecast.io
#Forecast.io API

import forecastio
import urllib2
import json

def get_weather_result(lat, lng):
	api_key = 'SECRET'

	forecast = forecastio.load_forecast(api_key, lat, lng)

	now = forecast.currently()
	by_hour = forecast.hourly()

	forecast_r = {"now_icon":now.icon, 
				  "now_summary":now.summary, 
				  "now_temp":now.temperature, 
				  "now_temp_feels":now.apparentTemperature, 
				  "hr_icon":by_hour.icon, 
				  "hr_summary":by_hour.summary}

	icon_img = {"clear-day":"wi wi-day-sunny",
				"clear-night": "wi wi-night-clear",
				"rain": "wi wi-rain",
				"snow": "wi wi-snow-wind",
				"sleet": "wi wi-day-sleet-storm",
				"wind": "wi wi-cloudy-windy",
				"fog": "wi wi-fog",
				"cloudy": "wi wi-cloudy",
				"partly-cloudy-day": "wi wi-day-cloudy",
				"partly-cloudy-night": "wi wi-night-partly-cloudy",
				"hail": "wi wi-hail",
				"thunderstorm": "wi wi-thunderstorm",
				"tornado": "wi wi-tornado"}

	forecast_r['now_icon'] = icon_img[now.icon]

	return forecast_r


URL_LATLONG = "http://www.trackip.net/ip?json"

def get_latlong():
	#content = None
	#try:
	#	content = urllib2.urlopen(URL_LATLONG).read()
	#except:
	#	return
	
	#get_data = json.loads(content)
	#lat,lng = get_data['latlong'].split(',')

	return 43.653226, -79.383184

#Wordnik API for define [word]
WORDNIK_KEY = 'SECRET'

def get_definition(word):
	word = word.split()[1]
	URL_DEF = "http://api.wordnik.com:80/v4/word.json/" + word + "/definitions?limit=200&includeRelated=true&sourceDictionaries=all&useCanonical=true&includeTags=false&api_key=" + WORDNIK_KEY

	content = None
	try:
		content = urllib2.urlopen(URL_DEF).read()
	except:
		print 0
	
	get_data = json.loads(content)
	
	define_r = []

	short_not = {"noun": 				"n.    ",
				 "verb": 				"v.    ",
				 "adjective":			"adj.  ",
				 "verb-transitive": 	"v.    ",
				 "verb-intransitive": 	"v.    ",
				 "idiom": 				"i.    ",
				 "preposition": 		"pre.  ",
				 "adverb": 				"adv.  ",
				 "pronoun": 			"pro.  ",
				 "determiner": 			"det.  ",
				 "conjunction": 		"conj. ",
				 "interjection": 		"ij.   ",
				 "abbreviation":		"ab.   ",
				 "affix":				"af.   ",
				 "article":				"ar.   ",
				 "auxiliary-verb":		"av.   ",
				 "definite-article":	"da.   ",
				 "phrasal-verb": 		"pv.   "}



	for i,item in enumerate(get_data):
		if i == 3:
			break
		try:
			if item["partOfSpeech"]:
				define_r.append([short_not[item["partOfSpeech"]], item['text']])
		except:
			pass

	return define_r

