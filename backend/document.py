""" Document class to store information about it"""
class document(object):
    
    def __init__(self,url):
        self._doc_url = url
        self._doc_title = ""
        self._doc_desc = []
        self._doc_score = .0
    
    # Getter and setter for url    
    def set_doc_url(self,url):
        self.doc_url = url
        
    def get_doc_url(self):
        return self._doc_url
    
    # Getter and setter for title     
    def set_doc_title(self,title):
        self._doc_title = title
        
    def get_doc_title(self):
        return self._doc_title
    
    # Getter and setter for description    
    def set_doc_desc(self,desc):
        self._doc_desc = desc
        
    def get_doc_desc(self):
        return "\n".join(self._doc_desc)
    
    # Getter and setter for score    
    def set_doc_score(self,score):
        self._doc_score = score
        
    def get_doc_score(self):
        return self._doc_score