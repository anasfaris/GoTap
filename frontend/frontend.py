#Google Login API
from oauth2client.client import OAuth2WebServerFlow 
from oauth2client.client import flow_from_clientsecrets 
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

import httplib2
import webbrowser
import time
import sys

#Gevent Server
import sys
if 'threading' in sys.modules:
    del sys.modules['threading']
from gevent import monkey; monkey.patch_all()

#Bottle Framework
from bottle import route, run, template, request, static_file, get, post, request, Bottle, error
import bottle

#Beaker Framework
from beaker.middleware import SessionMiddleware

#initialize important parameters for Google Login API
CLIENT_ID = 'SECRET'
CLIENT_SECRET = 'SECRET'
REDIRECT_URI = ''
if len(sys.argv) == 2:
    REDIRECT_URI = 'http://' + sys.argv[1] + '/redirect'
else:
    REDIRECT_URI = 'http://ec2-54-85-22-204.compute-1.amazonaws.com/redirect'
SCOPE = 'https://www.googleapis.com/auth/plus.me https://www.googleapis.com/auth/userinfo.email'

#initialize two dictionaries, one for the current keyword search
#the other for storing the whole history cache
history_cache = {}
keyword_cache = {}
recent_cache = {}
session = []

#Error pages

@error(404)
def error404(error):
	return template('views/error.html', error_msg="Such page or file does not exist")

@error(429)
def error404(error):
	return template('views/error.html', error_msg="Too many twitter request")

@error(405)
def error405(error):
	return template('views/error.html', error_msg="Method not allowed")

@error(503)
def error405(error):
	return template('views/error.html', error_msg="Too many instagram request")

#LAB 3 BEAUTIFUL SEARCH RESULT

from connect_db_sample import *
RESULT_PER_PAGE = 4

#LAB 4 APIs

from api import *
from features import *

def main_func(cur_user):

	#variable initialization
	math_result = "not_math"
	insta_result = None
	instagram_r = []
	forecast_r = {}
	define_r = []
	url_list_with_rank = []
	num_of_page = []
	shorten_result = []
	num_of_results = 0

	#clear the keyword cache to store only the keyword searched at the moment
	keyword_cache.clear()

	#get search from the input form name=keywords
	search = request.query.keywords
	page = request.query.page
	parse_search = search

	#check and fix if user enter just blank spaces as input
	count = 0
	for c in search:
		if c != " ":
			break
		else:
			count += 1

	if count == len(search):
		search = search.replace(" ", "")

	#check whether to use the math algorithm
	#don't use instagram if it pass the test because instagram
	#can't have symbol
	if parse_phrase(search) == "Math":
		try:
			math_result = do_math(search)
		except:
			pass
	elif search.lower() != "instagram":
		insta_result = True

	#check whether to define users word
	#user have to type define [word] for this to work
	if parse_phrase(search) == "Define":
		define_r = get_definition(search)

	#check whether user enter weather-like keyword
	#get the latitutde and longitude to get the weather
	if parse_phrase(search) == "Weather":
		lat, lng = get_latlong()
		if lat and lng:
			forecast_r = get_weather_result(lat, lng)
		#get_weather_result(lat,lng)

	#convert unicode string in page to integer
	if page:
		page_int = map(int,page)[0]
	else:
		page_int = 1

	#lab 3 - connect frontend with backend
	if search:
		#get the search suggestion using most titles of target and some important keyword
		suggested = search_suggestion(search)

		#get result from twitter and instagram API
		#current maximum API fetch for twitter is 450 per 15 minutes
		#current maximum API fetch for instagram is 5000 per hour
		twitter_r = get_twitter_result('#' + str(search))
		if insta_result:
			instagram_r = get_instagram_result(str(search))

		#convert search result to have '+' in between to send when necessary
		#for pagination, e.g search+param
		search_param = search.split()
		search_param = '+'.join(search_param)

		#uncomment this line to just query first word
		#word = str(search.lower()).split()[0]

		#get lexicon item from database
		try:
			for word in str(search.lower()).split():
				sorted_resolved_inverted_index_entry = sorted_resolved_inverted_index_db.get_item(word=word)
				url_ls = json.loads(sorted_resolved_inverted_index_entry['url_list'])
				title_ls = json.loads(sorted_resolved_inverted_index_entry['title_list'])
				score_ls = json.loads(sorted_resolved_inverted_index_entry['score_list'])

				for index,url in enumerate(url_ls):
					title = title_ls[index]
					score = float(score_ls[index])

					result_ls = [title,url,score]

					for i, item in enumerate(url_list_with_rank):
						if result_ls[1] == url_list_with_rank[i][1]:
							url_list_with_rank[i][2] += score
							result_ls[2] = url_list_with_rank[i][2]

					if result_ls not in url_list_with_rank:
						url_list_with_rank += [[title, url, score]]


			url_list_with_rank.sort(key=lambda sc: sc[2], reverse=True)

			num_of_results = len(url_list_with_rank)
			num_of_page = [i for i in range(1,(num_of_results)/RESULT_PER_PAGE+(num_of_results % RESULT_PER_PAGE > 0) + 1)]

			#useful for pagination
			page_int -= 1
			for j in range(RESULT_PER_PAGE):
				if (page_int * RESULT_PER_PAGE + j) == num_of_results:
					break
				shorten_result += [url_list_with_rank[page_int*RESULT_PER_PAGE+j]]

		except: pass
	else:
		word = ''
	
	#check if user signed in to update history for particular users
	if cur_user != '':
		signed_in = True
		#go to the function keyword_count to add/update value of dictionaries
		keyword_count(str(search).lower(), cur_user)
	else:
		signed_in = False
		#go to the function keyword_count to add/update value of dictionaries
		keyword_count(str(search).lower(), '')
	#create a new dictionary to get the top 20 keywords and 10 most recent keywords
	top_20 = {}
	recent_10 = {}

	#loop to get 20 keywords from history cache and store into top_20 cache
	i = 0;
	for word in sorted(history_cache, key=history_cache.get, reverse=True):
		top_20[word] = history_cache[word]
		i += 1
		if (i > 19):
			break
			
	#loop to get 20 keywords from history cache and store into recent_10 cache
	i = 0;
	for word_comma_user in sorted(recent_cache, key=recent_cache.get, reverse=False):
		word, user = str(word_comma_user).split(",")
		if user == cur_user:
			recent_10[word] = recent_cache[word_comma_user]	
			i += 1
			if (i > 9):
				break


	#if user don't search anything, show the main page
	if search == '':
		return template('views/query.html', 
			cur_user = cur_user,
			auto_comp = titles,
			history = history_cache,
			signed_in = signed_in)
	#otherwise, show the result page
	else:
		return template('views/result.html', 
			search_display = search, 
			keyword_cache = keyword_cache, 
			history = recent_10,
			cur_user = cur_user,
			cur_page = page_int+1,
			search_param = search_param,
			num_of_page = num_of_page,
			num_of_results = num_of_results,
			auto_comp = titles_js,
			url_list_with_rank = shorten_result,
			twitter_r = twitter_r,
			define_r = define_r,
			math_r = math_result,
			weather_r = forecast_r,
			instagram_r = instagram_r,
			suggested = suggested,
			signed_in = signed_in)


#function to add and update values for the two cache dictionaries
def keyword_count(search, user):
	if user:
		for word in search.split():
			if word not in history_cache:
				history_cache[word] = 1
			else:
				history_cache[word] = history_cache[word]+1
			if word not in keyword_cache:
				keyword_cache[word] = 1
			else:
				keyword_cache[word] = keyword_cache[word]+1
			#recent_cache will give a score from most recent (10) to least recent (0 and below)
			
			word_comma_user = word + ',' + user
			if word_comma_user not in recent_cache:
				recent_cache[word_comma_user] = 0, 1
			else:
				first, second = recent_cache[word_comma_user]
				recent_cache[word_comma_user] = 0, second+1
		for word_comma_user in recent_cache:
			first, second = recent_cache[word_comma_user]
			recent_cache[word_comma_user] = first+1, second
	else:
		for word in search.split():
			if word not in history_cache:
				history_cache[word] = 1
			else:
				history_cache[word] = history_cache[word]+1
			if word not in keyword_cache:
				keyword_cache[word] = 1
			else:
				keyword_cache[word] = keyword_cache[word]+1


#session management configuration using beaker framework
session_opts = {
    'session.type': 'file',
    'session.cookie_expires': 3600,
    'session.data_dir': './data',
    'session.auto': True
}
app = SessionMiddleware(bottle.app(), session_opts)

#specifying the path for the files
@bottle.route('/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='.')

#function that proceeds after successful login
@bottle.route('/redirect') 
def redirect_page():
	code = request.query.get('code', '')

	flow = OAuth2WebServerFlow(client_id=CLIENT_ID,
		client_secret=CLIENT_SECRET,
		scope=SCOPE,
		redirect_uri=REDIRECT_URI)
	credentials = flow.step2_exchange(code)
	token = credentials.id_token['sub']

	http = httplib2.Http()
	http = credentials.authorize(http)
	
	# Get user email
	users_service = build('oauth2', 'v2', http=http) 
	user_document = users_service.userinfo().get().execute() 
	user_email = user_document['email']
	cur_user = user_email

	session.append(1)
	session.reverse()

	s = bottle.request.environ.get('beaker.session')
	s['user'] = cur_user
	s.save()

	return main_func(s['user'])

#the main function
@bottle.route('/', 'GET')
def main():

	flow = flow_from_clientsecrets("client_secret.json", 
		scope=SCOPE, 
		redirect_uri=REDIRECT_URI)

	uri = flow.step1_get_authorize_url() 

	if request.GET.get('Sign-in'):
		bottle.redirect(str(uri))

	s = bottle.request.environ.get('beaker.session')

	if request.GET.get('Sign-out'):
		s.delete()
		session.append(0)
		session.reverse()
		#uncomment to just sign out from the webpage but not signing out google
		#return main_func(cur_user='')

		#signing out google
		return '<meta http-equiv="refresh" content="0; url=https://accounts.google.com/logout"/>'
	
	try:
		if session[0] == 0:
			cur_user=''
		else:
			cur_user=s['user']
		return main_func(cur_user)
	except:
		return main_func(cur_user='')

run(app, host='localhost', port=11080,server='gevent')
#run(app, host='0.0.0.0',port=80,server='gevent')

