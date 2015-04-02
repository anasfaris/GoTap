#function that detect user search, and return what the engine should do
def parse_phrase(search):
	math_component = "+-*/^1234567890()%. "
	valid_end = "0123456789)."
	valid_start = ".0123456789(+-"

	if search == '':
		return None

	if 'define' in search.lower()[:6] and len(search) > 7:
		return 'Define'

	if 'weather' in search.lower() or 'umbrella' in search.lower() or 'snow' in search.lower() or 'windy' in search.lower():
		return 'Weather'

	for letter in search:
		if letter not in math_component:
			return None

	if search[len(search)-1] not in valid_end:
		return None

	if search[0] not in valid_start:
		return None

	return "Math"

#evaluate math expression
def do_math(expression):
	return eval(expression.replace("^","**").replace("/","*1.0/"))

#to be used with autocompletion and search suggestion
titles = ['computer engineering research group',
		  'eecg student guide',
		  'computer group online thesis library',
		  'computer engineering group research facilities',
		  'computer engineering group graduate students',
		  'computer engineering group facility',
		  'university of toronto',
		  'recent news',
		  'uoft campus map',
		  'electrical & computer engineering',
		  'electrical and computer engineering',
		  'ece',
		  'electrical engineering',
		  'computer engineering',
		  'undergraduates in ece',
		  'alumni in uoft',
		  'research in uoft',
		  'programs in uoft',
		  'admission at uoft',
		  'jianwen zhu',
		  'tarek abdelrahman',
		  'parham aarabi',
		  'christiana amza',
		  'ashvin goel',
		  'farid najm',
		  'computer science',
		  'research facilities',
		  'human computer interaction',
		  'system software',
		  'compilers',
		  'computer architecture',
		  'vlsi cad',
		  'computer hardware',
		  'technical reports',
		  "how's the weather for today?",
		  "do i need an umbrella",
		  "what's the temperature outside",
		  "2 ^ 2",
		  "2 - 2",
		  "2 / 3",
		  "define word",
		  "define success",
		  'computer']

titles.sort()

import json
titles_js = json.dumps(titles)

#function to give some related search
def search_suggestion(phrase):
	suggested = {}
	for word in phrase.split():
		for i, item in enumerate(titles):
			if word in item.split():
				if titles[i] != phrase:
					if titles[i] not in suggested:
						suggested[titles[i]] = 1
					else:
						suggested[titles[i]] += 1

	suggested_ls =[]
	#loop to get 20 keywords from history cache and store into recent_10 cache
	for i, item in enumerate(sorted(suggested, key=suggested.get, reverse=True)):
		if i > 3:
			break
		suggested_ls.append(item)

	return suggested_ls

#uncomment to use spellchecker
#will make search engine a bit slow
'''
from bs4 import BeautifulSoup as bs
import urllib2

URL_START = "http://suggest.aspell.net/index.php?word="
URL_END = "&spelling=english&dict=normal&sugmode=slow"

def spell_check_phrase(phrase):
	new_phrase = ""
	new_word = None

	for word in phrase.split():
		try:
			new_word = spell_checker(word)
		except:
			new_word = None
		if new_word == None or new_word.lower() == word or word.lower() == 'uoft' or word.lower() == 'ece':
			new_phrase = new_phrase + " " + word
		else:
			new_phrase = new_phrase + " " + new_word

	if phrase == new_phrase[1:]:
		return None

	return new_phrase[1:]

def spell_checker(word):
	url = URL_START + word + URL_END
	soup = bs(urllib2.urlopen(url).read())
	tag = soup.find("a", target="aspell-def")
	return tag.string

'''
