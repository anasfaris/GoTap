frontend.py
	main file for the frontend
	contains main_func(), keyword_count(), and @bottle.route()
		-main_func() gets user input, parses it, gets the result from database and returns the result using html
		-keyword_count() gets user input and save user search history
		-@bottle.route() routes the url dynamically

features.py
	implement additional features for the website like math, auto-complete and search suggestion
	contains parse_phrase(), do_math(), search_suggestion()
		-parse_phrase() evaluates what the search engine will do (check weather, math function, or word definition)
		-do_math() does the math evaluation
		-search_suggestion() checks which keywords in the storage match the query phrases the most and returns the top four

api.py
	implement some of the search engine features using APIs like twitter, instagram, forecast.io, and wordnik
	contain get_twitter_result(), get_instagram_result(), list_banned(), get_weather_result(), get_latlong(), get_definition()
		-get_twitter_result() returns twitter hashtag or search result
		-get_instagram_result() returns instagram hashtag result
		-list_banned() bans offensive images from showing on the site
		-get_weather_result() returns weather result
		-get_latlong() returns latitute and longitude of user to get the weather result
		-get_definition() returns the words definition
