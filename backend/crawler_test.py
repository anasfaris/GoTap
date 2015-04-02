from crawler_mthread import crawler
from document import document
from pagerank import page_rank

def test_sorted_resolved_inverted_index():
    test_crawler = crawler(None, "")
    
    # test values
    WORD_A = "test"
    DOC_ID_A = 1
    DOC_ID_B = 2
    DOC_ID_C = 3
    DOC_ID_D = 4
    DOC_A = "http://www.A.com"
    DOC_B = "http://www.B.com"
    DOC_C = "http://www.C.com"
    DOC_D = "http://www.D.com"
    
    # Initialize crawler needed for the function call
    word_id = test_crawler.word_id(WORD_A)
    doc_id_list = []
    doc_id_list.append(DOC_ID_A)
    doc_id_list.append(DOC_ID_B)
    doc_id_list.append(DOC_ID_D)
    test_crawler.document_id(DOC_A)
    test_crawler.document_id(DOC_B)
    test_crawler.document_id(DOC_C)
    test_crawler.document_id(DOC_D)
    test_crawler._inverted_index[word_id] = doc_id_list
    test_crawler.add_link(DOC_ID_A, DOC_ID_B)
    test_crawler.add_link(DOC_ID_B, DOC_ID_D)
    test_crawler.add_link(DOC_ID_D, DOC_ID_C)
    test_crawler.compute_page_rank()
    test_crawler.construct_sorted_resolved_inverted_index()
    # Expected and actual result comparison
    expected_result = [document(DOC_D),document(DOC_B),document(DOC_A)]
    actual_result = test_crawler._sorted_resolved_inverted_index[WORD_A]

    # If the two results equal return true
    bool_A = (cmp(expected_result[0].get_doc_url(), actual_result[0].get_doc_url()) == 0)
    bool_B = (cmp(expected_result[1].get_doc_url(), actual_result[1].get_doc_url()) == 0)
    bool_C = (cmp(expected_result[2].get_doc_url(), actual_result[2].get_doc_url()) == 0)
    if bool_A and bool_B and bool_C:
        return True
    else:
        return False

def test_page_rank():
    test_crawler = crawler(None, "")
    
    # test values
    DOC_ID_A = 1
    DOC_ID_B = 2
    DOC_ID_C = 3
    DOC_ID_D = 4
    
    # Initialize crawler needed for the function call
    test_crawler.add_link(DOC_ID_A, DOC_ID_B)
    test_crawler.add_link(DOC_ID_B, DOC_ID_D)
    test_crawler.add_link(DOC_ID_D, DOC_ID_C)
    test_crawler.compute_page_rank()
    # Expected and actual result comparison
    expected_result = page_rank([(DOC_ID_A,DOC_ID_B), (DOC_ID_B, DOC_ID_D), (DOC_ID_D, DOC_ID_C)])
    actual_result = test_crawler._page_rank

    # If the two results equal return true
    if cmp(expected_result, actual_result) == 0:
        return True
    else:
        return False

def test_lexicon():
    test_crawler = crawler(None, "")
    
    # test values
    WORD_A = "Hello"
    WORD_ID_A = 1
    WORD_B = "World"
    WORD_ID_B = 2
    
    # Initialize crawler needed for the function call
    test_crawler.word_id(WORD_A)
    test_crawler.word_id(WORD_B)
    # Expected and actual result comparison
    expected_result = {WORD_A:WORD_ID_A,WORD_B:WORD_ID_B}
    actual_result = test_crawler._word_id_cache

    # If the two results equal return true
    if cmp(expected_result, actual_result) == 0:
        return True
    else:
        return False

def test_document_index():
    test_crawler = crawler(None, "")
    
    # test values
    URL_A = "http://www.testA.com"
    DOC_ID_A = 1
    URL_B = "http://www.testB.com"
    DOC_ID_B = 2
    
    # Initialize crawler needed for the function call
    test_crawler.document_id(URL_A)
    test_crawler.document_id(URL_B)
    # Expected and actual result comparison
    expected_result = {DOC_ID_A:document(URL_A),DOC_ID_B:document(URL_B)}
    actual_result = test_crawler._document_index

    # If the two results equal return true
    bool_A = (cmp(expected_result[DOC_ID_A].get_doc_url(), actual_result[DOC_ID_A].get_doc_url()) == 0)
    bool_B = (cmp(expected_result[DOC_ID_B].get_doc_url(), actual_result[DOC_ID_B].get_doc_url()) == 0)
    if bool_A and bool_B:
        return True
    else:
        return False

def test_get_inverted_index():
    test_crawler = crawler(None, "")
    
    # test values
    WORD_ID_A = "1"
    WORD_ID_B = "2"
    WORD_ID_C = "3"
    FONT_A = 0
    FONT_B = 0
    FONT_C = 0
    DOC_ID_A = "1"
    
    # Initialize crawler needed for the function call
    curr_words = ( (WORD_ID_A,FONT_A), (WORD_ID_B,FONT_B), (WORD_ID_C,FONT_C) )
    curr_doc_id = DOC_ID_A
    test_crawler.add_words_to_inverted_index(curr_words,curr_doc_id)
    
    # Expected and actual result comparison
    expected_result = {WORD_ID_A:{DOC_ID_A},WORD_ID_B:{DOC_ID_A},WORD_ID_C:{DOC_ID_A}}
    actual_result = test_crawler.get_inverted_index()
    
    # If the two results equal return true
    if cmp(expected_result, actual_result) == 0:
        return True
    else:
        return False

def test_get_resolved_inverted_index():
    test_crawler = crawler(None, "")
    
    # test values
    WORD_ID_A = "1"
    WORD_ID_B = "2"
    WORD_ID_C = "3"
    WORD_A = "I"
    WORD_B = "am"
    WORD_C = "Groot"
    FONT_A = 0
    FONT_B = 0
    FONT_C = 0
    DOC_ID_A = "1"
    URL_A = "http://www.test.com"
    
    # Initialize crawler needed for the function call
    test_crawler._inverted_lexicon = {WORD_ID_A:WORD_A,WORD_ID_B:WORD_B,WORD_ID_C:WORD_C}
    test_crawler._document_index = {DOC_ID_A:document(URL_A)}
    curr_words = ( (WORD_ID_A,FONT_A), (WORD_ID_B,FONT_B), (WORD_ID_C,FONT_C) )
    curr_doc_id = DOC_ID_A
    test_crawler.add_words_to_inverted_index(curr_words,curr_doc_id)
    
    # Expected and actual result comparison
    expected_result = {WORD_A:{URL_A},WORD_B:{URL_A},WORD_C:{URL_A}}
    actual_result = test_crawler.get_resolved_inverted_index()
    
    # If the two results equal return true
    if cmp(expected_result, actual_result) == 0:
        return True
    else:
        return False

def  test_database():
    import boto.dynamodb2
    from boto.dynamodb2.fields import HashKey
    from boto.dynamodb2.table import Table
    from boto.dynamodb2.types import NUMBER, STRING
    import json
    
    sorted_resolved_inverted_index_db = Table('sorted_resolved_inverted_index', 
                     schema=[HashKey('word', data_type=STRING),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id="SECRET",
                                                                 aws_secret_access_key="SECRET")
                    )
    DOC_A = "http://www.eecg.toronto.edu/Welcome.html"
    DOC_B = "http://www.ece.utoronto.ca"
    DOC_C = "http://www.eecg.toronto.edu"
    WORD_A = "experience"
    # Expected and actual result comparison
    expected_result = [DOC_A,DOC_B,DOC_C]
    actual_result = sorted_resolved_inverted_index_db.get_item(word=WORD_A)
    actual_result = json.loads(actual_result['url_list']);
    
    # If the two results equal return true
    bool_A = (cmp(expected_result[0], actual_result[0]) == 0)
    bool_B = (cmp(expected_result[1], actual_result[1]) == 0)
    bool_C = (cmp(expected_result[2], actual_result[2]) == 0)
    if bool_A and bool_B and bool_C:
        return True
    else:
        return False
 
 
print "===Test Result===" 
print "get_inverted_index(): ", test_get_inverted_index()
print "get_resolved_inverted_index(): ", test_get_resolved_inverted_index() 
print "_document_index: ", test_document_index()
print "_lexicon: ", test_lexicon()
print "_page_rank: ", test_page_rank()
print "_sorted_resolved_inverted_index: ", test_sorted_resolved_inverted_index()
print "Database access: ", test_database()         
