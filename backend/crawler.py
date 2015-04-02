
# Copyright (C) 2011 by Peter Goodman
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import urllib2
import urlparse
from bs4 import BeautifulSoup
from bs4 import Tag
from collections import defaultdict
import re
# create class for document object
from document import document
from pagerank import page_rank

from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER,STRING
import json

def attr(elem, attr):
    """An html attribute from an html element. E.g. <a href="">, then
    attr(elem, "href") will get the href or an empty string."""
    try:
        return elem[attr]
    except:
        return ""

WORD_SEPARATORS = re.compile(r'\s|\n|\r|\t|[^a-zA-Z0-9\-_]')

class crawler(object):
    """Represents 'Googlebot'. Populates a database by crawling and indexing
    a subset of the Internet.

    This crawler keeps track of font sizes and makes it simpler to manage word
    ids and document ids."""

    def __init__(self, db_conn, url_file):
        """Initialize the crawler with a connection to the database to populate
        and with the file containing the list of seed URLs to begin indexing."""
        self._url_queue = [ ]
        self._doc_id_cache = { }
        self._word_id_cache = { }
        # Data structure for inverted index, lexicon and document index
        self._inverted_index = { }
        self._document_index = { }
        self._inverted_lexicon = { }
        self._page_rank_list = [ ]
        self._page_rank = { }
        self._sorted_resolved_inverted_index = { }
        # Initialize database for crawler
        self._lexicon_db = Table('lexicon', 
                     schema=[HashKey('word', data_type=STRING),])
        self._document_index_db = Table('document_index', 
                     schema=[HashKey('doc_id', data_type=NUMBER),])
        self._inverted_index_db = Table('inverted_index', 
                     schema=[HashKey('word_id', data_type=NUMBER),])
        self._page_rank_db = Table('page_rank', 
                     schema=[HashKey('doc_id', data_type=NUMBER),])
        self._sorted_resolved_inverted_index_db = Table('sorted_resolved_inverted_index', 
                                                        schema=[HashKey('word', data_type=STRING),])

        # functions to call when entering and exiting specific tags
        self._enter = defaultdict(lambda *a, **ka: self._visit_ignore)
        self._exit = defaultdict(lambda *a, **ka: self._visit_ignore)

        # add a link to our graph, and indexing info to the related page
        self._enter['a'] = self._visit_a

        # record the currently indexed document's title an increase
        # the font size
        def visit_title(*args, **kargs):
            self._visit_title(*args, **kargs)
            self._increase_font_factor(7)(*args, **kargs)

        # increase the font size when we enter these tags
        self._enter['b'] = self._increase_font_factor(2)
        self._enter['strong'] = self._increase_font_factor(2)
        self._enter['i'] = self._increase_font_factor(1)
        self._enter['em'] = self._increase_font_factor(1)
        self._enter['h1'] = self._increase_font_factor(7)
        self._enter['h2'] = self._increase_font_factor(6)
        self._enter['h3'] = self._increase_font_factor(5)
        self._enter['h4'] = self._increase_font_factor(4)
        self._enter['h5'] = self._increase_font_factor(3)
        self._enter['title'] = visit_title

        # decrease the font size when we exit these tags
        self._exit['b'] = self._increase_font_factor(-2)
        self._exit['strong'] = self._increase_font_factor(-2)
        self._exit['i'] = self._increase_font_factor(-1)
        self._exit['em'] = self._increase_font_factor(-1)
        self._exit['h1'] = self._increase_font_factor(-7)
        self._exit['h2'] = self._increase_font_factor(-6)
        self._exit['h3'] = self._increase_font_factor(-5)
        self._exit['h4'] = self._increase_font_factor(-4)
        self._exit['h5'] = self._increase_font_factor(-3)
        self._exit['title'] = self._increase_font_factor(-7)

        # never go in and parse these tags
        self._ignored_tags = set([
            'meta', 'script', 'link', 'meta', 'embed', 'iframe', 'frame', 
            'noscript', 'object', 'svg', 'canvas', 'applet', 'frameset', 
            'textarea', 'style', 'area', 'map', 'base', 'basefont', 'param',
        ])

        # set of words to ignore
        self._ignored_words = set([
            '', 'the', 'of', 'at', 'on', 'in', 'is', 'it',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
            'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z', 'and', 'or',
        ])

        # TODO remove me in real version
        self._mock_next_doc_id = 1
        self._mock_next_word_id = 1

        # keep track of some info about the page we are currently parsing
        self._curr_depth = 0
        self._curr_url = ""
        self._curr_doc_id = 0
        self._font_size = 0
        self._curr_words = None

        # get all urls into the queue
        try:
            with open(url_file, 'r') as f:
                for line in f:
                    self._url_queue.append((self._fix_url(line.strip(), ""), 0))
        except IOError:
            pass
    
    # TODO remove me in real version
    def _mock_insert_document(self, url):
        """A function that pretends to insert a url into a document db table
        and then returns that newly inserted document's id."""
        ret_id = self._mock_next_doc_id
        self._mock_next_doc_id += 1
        return ret_id
    
    # TODO remove me in real version
    def _mock_insert_word(self, word):
        """A function that pretends to inster a word into the lexicon db table
        and then returns that newly inserted word's id."""
        ret_id = self._mock_next_word_id
        self._mock_next_word_id += 1
        return ret_id
    
    def word_id(self, word):
        """Get the word id of some specific word."""
        if word in self._word_id_cache:
            return self._word_id_cache[word]
        
        # TODO:
        #       query the lexicon for the id assigned to this word, 
        #       store it in the word id cache, and return the id.
        word_id = self._mock_insert_word(word)
        self._word_id_cache[word] = word_id
        # add the word to the lexicon
        self._inverted_lexicon[word_id] = word
        return word_id
    
    def document_id(self, url):
        """Get the document id for some url."""
        if url in self._doc_id_cache:
            return self._doc_id_cache[url]
        
        # TODO: just like word id cache, but for documents. if the document
        #       doesn't exist in the db then only insert the url and leave
        #       the rest to their defaults.
        
        doc_id = self._mock_insert_document(url)
        self._doc_id_cache[url] = doc_id
        
        # add the newly created document object to the document index
        self._document_index[doc_id] = document(url)
        #self._document_index[doc_id] = url
        return doc_id
    
    def _fix_url(self, curr_url, rel):
        """Given a url and either something relative to that url or another url,
        get a properly parsed url."""

        rel_l = rel.lower()
        if rel_l.startswith("http://") or rel_l.startswith("https://"):
            curr_url, rel = rel, ""
            
        # compute the new url based on import 
        curr_url = urlparse.urldefrag(curr_url)[0]
        parsed_url = urlparse.urlparse(curr_url)
        return urlparse.urljoin(parsed_url.geturl(), rel)

    def add_link(self, from_doc_id, to_doc_id):
        """Add a link into the database, or increase the number of links between
        two pages in the database."""
        # TODO
        self._page_rank_list.append((from_doc_id, to_doc_id))
        

    def _visit_title(self, elem):
        """Called when visiting the <title> tag."""
        title_text = self._text_of(elem).strip()
        #print "document title="+ repr(title_text)

        # update document title for document id self._curr_doc_id
        #self._document_index[self._curr_doc_id].set_doc_title(title_text)
    
    def _visit_a(self, elem):
        """Called when visiting <a> tags."""

        dest_url = self._fix_url(self._curr_url, attr(elem,"href"))

        #print "href="+repr(dest_url), \
        #      "title="+repr(attr(elem,"title")), \
        #      "alt="+repr(attr(elem,"alt")), \
        #      "text="+repr(self._text_of(elem))

        # add the just found URL to the url queue
        self._url_queue.append((dest_url, self._curr_depth))
        
        # add a link entry into the database from the current document to the
        # other document
        self.add_link(self._curr_doc_id, self.document_id(dest_url))
        
        # create a tuple of current doc id and destination doc id and append it into page_rank_list

        # TODO add title/alt/text to index for destination url
    
    def _add_words_to_document(self):
        # TODO: knowing self._curr_doc_id and the list of all words and their
        #       font sizes (in self._curr_words), add all the words into the
        #       database for this document
        #print "    num words="+ str(len(self._curr_words))
        pass

    def _increase_font_factor(self, factor):
        """Increade/decrease the current font size."""
        def increase_it(elem):
            self._font_size += factor
        return increase_it
    
    def _visit_ignore(self, elem):
        """Ignore visiting this type of tag"""
        pass

    def _add_text(self, elem):
        """Add some text to the document. This records word ids and word font sizes
        into the self._curr_words list for later processing."""       
        words = WORD_SEPARATORS.split(elem.string.lower())
        for word in words:
            word = word.strip()
            if word in self._ignored_words:
                continue
            self._curr_words.append((self.word_id(word), self._font_size))
        
    def _text_of(self, elem):
        """Get the text inside some element without any tags."""
        if isinstance(elem, Tag):
            text = [ ]
            for sub_elem in elem:
                text.append(self._text_of(sub_elem))
            
            return " ".join(text)
        else:
            return elem.string

    def _index_document(self, soup):
        """Traverse the document in depth-first order and call functions when entering
        and leaving tags. When we come accross some text, add it into the index. This
        handles ignoring tags that we have no business looking at."""
        class DummyTag(object):
            next = False
            name = ''
        
        class NextTag(object):
            def __init__(self, obj):
                self.next = obj
        
        self._add_title_to_doc_index(soup)
        tag = soup.html
        stack = [DummyTag(), soup.html]

        while tag and tag.next:
            tag = tag.next

            # html tag
            if isinstance(tag, Tag):

                if tag.parent != stack[-1]:
                    self._exit[stack[-1].name.lower()](stack[-1])
                    stack.pop()

                tag_name = tag.name.lower()

                # ignore this tag and everything in it
                if tag_name in self._ignored_tags:
                    if tag.nextSibling:
                        tag = NextTag(tag.nextSibling)
                    else:
                        self._exit[stack[-1].name.lower()](stack[-1])
                        stack.pop()
                        tag = NextTag(tag.parent.nextSibling)
                    
                    continue
                
                # enter the tag
                self._enter[tag_name](tag)
                stack.append(tag)

            # text (text, cdata, comments, etc.)
            else:
                self._add_text(tag)
                #self._add_desc_to_doc_index(tag)

    def _add_title_to_doc_index(self, soup):
        self._document_index[self._curr_doc_id].set_doc_title(soup.title.string)

    def _add_desc_to_doc_index(self, elem):
        """Add first 3 lines of text to the document object"""       
        # Get the whole text with capital letters
        orig_words = WORD_SEPARATORS.split(elem.string)
        # Ignore empty string
        temp = [word for word in orig_words if word != ""]    
        if len(temp) > 0:
            desc = self._document_index[self._curr_doc_id]._doc_desc
            # Just store the first 3 lines
            if len(desc) < 3:
                desc.append(" ".join(temp))
            self._document_index[self._curr_doc_id].set_doc_desc(desc)
     
    def _add_words_to_inverted_index(self):
        """Add word id as key and list of doc id as value"""
        for word,font in self._curr_words:
            # Word is inside the dictionary
            if word in self.get_inverted_index():
                doc_id_list = self.get_inverted_index()[word]
                # If the current doc id is not inside the list, append it to the list
                if not self._curr_doc_id in doc_id_list:
                    doc_id_list.add(self._curr_doc_id)
                    self.get_inverted_index()[word] = doc_id_list
            # Word not in the dictionary, add the new entry of word id as key and list of doc id as value
            else:
                temp_set = set()
                temp_set.add(self._curr_doc_id)
                self.get_inverted_index()[word] = temp_set

    def get_inverted_index(self):
        """return inverted index as dict() with key = word id, value = list of doc id"""
        return self._inverted_index
    
    def get_resolved_inverted_index(self):
        """return resolved inverted index as dict() with key = word, value = list of url"""
        result = dict()
        # Iterate through the keys of inverted_index
        for word_id in self.get_inverted_index():
            doc = set()
            doc_id_set = self.get_inverted_index()[word_id]
            # Translate the word id into word using lexicon
            translated_word = self._inverted_lexicon[word_id].encode("ascii")
            for doc_id in doc_id_set:
                # For each doc id, translate it to url using document index
                translated_doc = self._document_index[doc_id]
                doc.add(translated_doc._doc_url.encode("ascii"))
                #doc.add(translated_doc.encode("ascii"))
            result[translated_word] = doc
        return result

    def crawl(self, depth=2, timeout=3):
        """Crawl the web!"""
        seen = set()

        while len(self._url_queue):

            url, depth_ = self._url_queue.pop()
            # skip this url; it's too deep
            if depth_ > depth:
                continue

            doc_id = self.document_id(url)

            # we've already seen this document
            if doc_id in seen:
                continue

            seen.add(doc_id) # mark this document as haven't been visited
            
            socket = None
            try:
                socket = urllib2.urlopen(url, timeout=timeout)
                soup = BeautifulSoup(socket.read())

                self._curr_depth = depth_ + 1
                self._curr_url = url
                self._curr_doc_id = doc_id
                self._font_size = 0
                self._curr_words = [ ]
                self._index_document(soup)
                self._add_words_to_document()
                #print "    url="+repr(self._curr_url)
                self._add_words_to_inverted_index()

            except Exception as e:
                #print "Exception: ", e
                pass
            finally:
                if socket:
                    socket.close()
        
    def compute_page_rank(self):
        self._page_rank = page_rank(self._page_rank_list)
    
    def construct_sorted_resolved_inverted_index(self):
        '''Optimized structure so that only single access 
            is needed to find the list of url sorted by rank score'''
        for word,word_id in self._word_id_cache.iteritems():
            temp_list = []
            for doc_id in self._inverted_index[word_id]:
                temp_list.append([doc_id,self._page_rank[doc_id]])
            # Sort the list of url based on page rank score
            temp_list.sort(key=lambda rank: rank[1], reverse=True)
            sorted_list = []
            for entry in temp_list:
                doc_id = entry[0]
                self._document_index[doc_id].set_doc_score(self._page_rank[doc_id])
                # Append tbe document object into the sorted list
                sorted_list.append(self._document_index[doc_id])
            self._sorted_resolved_inverted_index[word] = sorted_list
                    
                    
    def persist_data(self):
        # Persist lexicon data into database
        for word,word_id in self._word_id_cache.iteritems():
            self._lexicon_db.put_item(data={
                                            'word': word,
                                            'word_id': word_id,
                                            })
        # Persist document index data into database
        for doc_id,doc in self._document_index.iteritems():
            self._document_index_db.put_item(data={
                                                   'doc_id': doc_id,
                                                   #'url': doc,
                                                   'url': doc.get_doc_url(),
                                                   #'title': doc.get_doc_title(),
                                                   #'score': str(doc.get_doc_score()),
                                                   #'desc': doc.get_doc_desc(),
                                                   })
        # Persist inverted index data into database
        for word_id,doc_id_set in self._inverted_index.iteritems():
            self._inverted_index_db.put_item(data={
                                                   'word_id': word_id,
                                                   'doc_id': json.dumps(list(doc_id_set))
                                                   })
                                                   
        # Persist page rank data into data
        for doc_id,score in self._page_rank.iteritems():
            self._page_rank_db.put_item(data={
                                              'doc_id': doc_id,
                                              'score': str(score),
                                              })
            
        # Persist sorted inverted index data into data
        for word,sorted_list in self._sorted_resolved_inverted_index.iteritems():
            url_list = []
            title_list = []
            score_list = []
            for doc in sorted_list:
                url_list.append(doc.get_doc_url())
                title_list.append(doc.get_doc_title())
                score_list.append(str(doc.get_doc_score()))
            self._sorted_resolved_inverted_index_db.put_item(data={
                                              'word': word,
                                              'url_list': json.dumps(url_list),
                                              'title_list': json.dumps(title_list),
                                              'score_list': json.dumps(score_list),
                                              })
      

if __name__ == "__main__":
    bot = crawler(None, "urls.txt")
    bot.crawl(depth=1)
    bot.compute_page_rank()
    bot.construct_sorted_resolved_inverted_index()
    print bot._sorted_resolved_inverted_index["bldg"]
    #bot.persist_data()
    #print bot._sorted_resolved_inverted_index
    #print bot.get_inverted_index()
    #print bot.get_resolved_inverted_index()

