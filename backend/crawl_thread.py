from threading import Thread
import urllib2
from bs4 import BeautifulSoup
from bs4 import Tag
from collections import defaultdict
import re

def attr(elem, attr):
    """An html attribute from an html element. E.g. <a href="">, then
    attr(elem, "href") will get the href or an empty string."""
    try:
        return elem[attr]
    except:
        return ""

WORD_SEPARATORS = re.compile(r'\s|\n|\r|\t|[^a-zA-Z0-9\-_]')

class crawl_thread(Thread):
    def __init__(self,crawler,url,depth_,depth=2,timeout=3):
        Thread.__init__(self)
        self._crawler = crawler
        self._url = url
        self._depth = depth_
        self._req_depth = depth
        self._timeout = timeout
        
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
    
    def _visit_title(self, elem):
        """Called when visiting the <title> tag."""
        title_text = self._text_of(elem).strip()
        #print "document title="+ repr(title_text)
    
    def _add_desc_to_doc_index(self, elem):
        """Add first 3 lines of text to the document object"""       
        # Get the whole text with capital letters
        orig_words = WORD_SEPARATORS.split(elem.string)
        # Ignore empty string
        temp = [word for word in orig_words if word != ""]    
        if len(temp) > 0:
            desc = self._crawler._document_index[self._doc_id]._doc_desc
            # Just store the first 3 lines
            if len(desc) < 3:
                desc.append(" ".join(temp))
            self._crawler._document_index[self._doc_id].set_doc_desc(desc)
    
    def _visit_a(self, elem):
        """Called when visiting <a> tags."""

        dest_url = self._crawler._fix_url(self._url, attr(elem,"href"))

        #print "href="+repr(dest_url), \
        #      "title="+repr(attr(elem,"title")), \
        #      "alt="+repr(attr(elem,"alt")), \
        #      "text="+repr(self._text_of(elem))

        # add the just found URL to the url queue
        self._crawler._url_queue.append((dest_url, self._depth))
        
        # add a link entry into the database from the current document to the
        # other document
        dest_doc_id = self._crawler.document_id(dest_url)
        self._crawler.add_link(self._doc_id, dest_doc_id)
        
        # create a tuple of current doc id and destination doc id and append it into page_rank_list

        # TODO add title/alt/text to index for destination url
    
    def _increase_font_factor(self, factor):
        """Increase/decrease the current font size."""
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
            self._words.append((self._crawler.word_id(word), self._font_size))
        
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
        
        self._crawler._add_title_to_doc_index(soup,self._doc_id)
        
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
        
    def run(self):
        # skip this url; it's too deep
        if self._depth > self._req_depth:
            return

        doc_id = self._crawler.document_id(self._url)
        # we've already seen this document, end the thread
        if self._crawler.check_doc_in_seen(doc_id):
            return
        
        socket = None
        try:
            socket = urllib2.urlopen(self._url, timeout=self._timeout)
            soup = BeautifulSoup(socket.read())

            self._depth += 1
            self._doc_id = doc_id
            self._font_size = 0
            self._words = [ ]
            self._index_document(soup)
            #print "    url="+repr(self._url)
            self._crawler.add_words_to_inverted_index(self._words,self._doc_id)

        except Exception as e:
            #print "Exception: ", e
            pass
        finally:
            if socket:
                socket.close()
