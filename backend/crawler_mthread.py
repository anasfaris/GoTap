
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

import json
import threading
import urlparse

from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER,STRING

from crawl_thread import crawl_thread
from document import document
from pagerank import page_rank

class crawler(object):
    """Represents 'Googlebot'. Populates a database by crawling and indexing
    a subset of the Internet.

    This crawler keeps track of font sizes and makes it simpler to manage word
    ids and document ids."""

    # Static lock variables for each shared data structures by all the crawler threads
    doc_id_lock = threading.Lock()
    word_id_lock = threading.Lock()
    seen_lock = threading.Lock()
    inverted_index_lock = threading.Lock()

    def __init__(self, db_conn, url_file):
        """Initialize the crawler with a connection to the database to populate
        and with the file containing the list of seed URLs to begin indexing."""
        self._url_queue = [ ]
        self._doc_id_cache = { }
        self._word_id_cache = { }
        # Data structure for inverted index, lexicon and document index
        self._inverted_lexicon = { }
        self._inverted_index = { }
        self._document_index = { }
        self._page_rank_list = [ ]
        self._page_rank = { }
        self._seen = set()
        self._sorted_resolved_inverted_index = { }
        # Initialize database for crawler
        self._lexicon_db = Table('lexicon', 
                     schema=[HashKey('word_id', data_type=NUMBER),])
        self._document_index_db = Table('document_index', 
                     schema=[HashKey('doc_id', data_type=NUMBER),])
        self._inverted_index_db = Table('inverted_index', 
                     schema=[HashKey('word_id', data_type=NUMBER),])
        self._page_rank_db = Table('page_rank', 
                     schema=[HashKey('doc_id', data_type=NUMBER),])
        self._sorted_resolved_inverted_index_db = Table('sorted_resolved_inverted_index', 
                                                        schema=[HashKey('word', data_type=STRING),])

        # TODO remove me in real version
        self._mock_next_doc_id = 1
        self._mock_next_word_id = 1

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
        """A function that pretends to insert a word into the lexicon db table
        and then returns that newly inserted word's id."""
        ret_id = self._mock_next_word_id
        self._mock_next_word_id += 1
        return ret_id
    
    def word_id(self, word):
        """Get the word id of some specific word."""
        # Acquire word_id_lock before enter critical section
        crawler.word_id_lock.acquire()
        if word in self._word_id_cache:
            # Release word_id_lock before return
            crawler.word_id_lock.release()
            return self._word_id_cache[word]
        
        # TODO:
        #       query the lexicon for the id assigned to this word, 
        #       store it in the word id cache, and return the id.
        word_id = self._mock_insert_word(word)
        self._word_id_cache[word] = word_id
        # Release word_id_lock
        crawler.word_id_lock.release()
        # Add the word and its word_id into inverted lexicon
        self._inverted_lexicon[word_id] = word
        return word_id
    
    def document_id(self, url):
        """Get the document id for some url."""
        # Acquire doc_id_lock before enter critical section
        crawler.doc_id_lock.acquire()
        if url in self._doc_id_cache:
            # Release doc_id_lock before return
            crawler.doc_id_lock.release()
            return self._doc_id_cache[url]
        
        # TODO: just like word id cache, but for documents. if the document
        #       doesn't exist in the db then only insert the url and leave
        #       the rest to their defaults.
        
        doc_id = self._mock_insert_document(url)
        self._doc_id_cache[url] = doc_id
        # Release doc_id_lock
        crawler.doc_id_lock.release()
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
        self._page_rank_list.append((from_doc_id, to_doc_id))
    
    def _add_words_to_document(self):
        # TODO: knowing self._curr_doc_id and the list of all words and their
        #       font sizes (in self._curr_words), add all the words into the
        #       database for this document
        #print "    num words="+ str(len(self._curr_words))
        pass
    
    def _add_title_to_doc_index(self, soup,curr_doc_id):
        self._document_index[curr_doc_id].set_doc_title(soup.title.string)
     
    def add_words_to_inverted_index(self,words,doc_id):
        """Add word id as key and list of doc id as value"""
        for word,font in words:
            # Word is inside the dictionary
            # Acquire inverted_index_lock before enter critical section
            crawler.inverted_index_lock.acquire()
            if word in self.get_inverted_index():
                doc_id_list = self.get_inverted_index()[word]
                # If the current doc id is not inside the list, append it to the list
                if not doc_id in doc_id_list:
                    doc_id_list.add(doc_id)
                    self.get_inverted_index()[word] = doc_id_list
            # Word not in the dictionary, add the new entry of word id as key and list of doc id as value
            else:
                temp_set = set()
                temp_set.add(doc_id)
                self.get_inverted_index()[word] = temp_set
            # Release inverted_index_lock
            crawler.inverted_index_lock.release()

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
        """Create a new thread for each new url in the queue and do the crawling simultaneously"""
        # Count the number of thread before start crawling
        threadNum = threading.active_count()
        
        while True:
            if len(self._url_queue) > 0:
                
                url, depth_ = self._url_queue.pop()
                
                # Start a new thread for each url retrieved for the queue
                crawl_thread(self,url,depth_,depth,timeout).start()
            
            # Break out of the loop if the queue is empty and all the created threads has completed
            if len(self._url_queue) == 0 and threadNum == threading.active_count():
                break  
    
    def compute_page_rank(self):
        """Call the page rank function with the _page_rank_list input to compute the score for each doc"""
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
        """Store all the important data structure (lexicon, inverted index, page rank and document index into database"""
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
                                                   'url': doc.get_doc_url(),
                                                   #'title': doc.get_doc_title(),
                                                   #'desc': doc.get_doc_desc(),
                                                   })
        # Persist inverted index data into database
        for word_id,doc_id_set in self._inverted_index.iteritems():
            self._inverted_index_db.put_item(data={
                                                   'word_id': word_id,
                                                   # Store the list of doc_id as a JSON string
                                                   'doc_id': json.dumps(list(doc_id_set))
                                                   })
                                                   
        # Persist page rank data into data
        for doc_id,score in self._page_rank.iteritems():
            self._page_rank_db.put_item(data={
                                              'doc_id': doc_id,
                                              # Store the float of score as string
                                              'score': str(score),
                                              })
            
        # Persist sorted resolved inverted index data into data
        for word,sorted_list in self._sorted_resolved_inverted_index.iteritems():
            url_list = []
            title_list = []
            score_list = []
            for doc in sorted_list:
                # Construct url list, title list and score list
                url_list.append(doc.get_doc_url())
                title_list.append(doc.get_doc_title())
                score_list.append(str(doc.get_doc_score()))
            self._sorted_resolved_inverted_index_db.put_item(data={
                                              'word': word,
                                              'url_list': json.dumps(url_list),
                                              'title_list': json.dumps(title_list),
                                              'score_list': json.dumps(score_list),
                                              })
                                              
    def check_doc_in_seen(self,doc_id):
        """Check if the document associated wih the doc_id has been visited or not"""
        # Acquire seen_lock before enter critical section
        crawler.seen_lock.acquire()
        if doc_id in self._seen:
            retval = True
        else:
            retval = False
            # Insert the doc_id into seen if it haven't been visited before
            self._seen.add(doc_id)
        # Release seen_lock
        crawler.seen_lock.release()
            
        return retval
         

if __name__ == "__main__":
    bot = crawler(None, "urls.txt")
    bot.crawl(depth=1)
    bot.compute_page_rank()
    bot.construct_sorted_resolved_inverted_index()
    bot.persist_data()

