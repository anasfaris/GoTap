import boto.dynamodb2
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER, STRING
import json

""" Connect to the databases that I have created """
lexicon_db = Table('lexicon', 
                     schema=[HashKey('word', data_type=STRING),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id='SECRET',
                                                                 aws_secret_access_key='SECRET')
                    )

inverted_index_db = Table('inverted_index', 
                     schema=[HashKey('word_id', data_type=NUMBER),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id='SECRET',
                                                                 aws_secret_access_key='SECRET')
                    )

page_rank_db = Table('page_rank', 
                     schema=[HashKey('doc_id', data_type=NUMBER),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id='SECRET',
                                                                 aws_secret_access_key='SECRET')
                    )

document_index_db = Table('document_index', 
                     schema=[HashKey('doc_id', data_type=NUMBER),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id='SECRET',
                                                                 aws_secret_access_key='SECRET')
                    )

sorted_resolved_inverted_index_db = Table('sorted_resolved_inverted_index', 
                     schema=[HashKey('word', data_type=STRING),],
                     # If you need to specify custom parameters like keys or region info
                    connection= boto.dynamodb2.connect_to_region(region_name="us-east-1",
                                                                 aws_access_key_id='SECRET',
                                                                 aws_secret_access_key='SECRET')
                    )

'''
""" Get Lexicon Item """
lexicon_entry = lexicon_db.get_item(word='annex')
lexicon_entry_word = lexicon_entry['word']
lexicon_entry_id = lexicon_entry['word_id']
print "word: ",lexicon_entry_word, " ", "id: ", lexicon_entry_id

""" Get Inverted Index Item """
inverted_index_entry = inverted_index_db.get_item(word_id=lexicon_entry_id)
inverted_index_entry_word_id = inverted_index_entry['word_id']
inverted_index_entry_doc_id = inverted_index_entry['doc_id']
# Decode the json string into a list
print "word id: ",inverted_index_entry_word_id, " ", "list of doc id: ", json.loads(inverted_index_entry_doc_id)

""" Get Page Rank Item """
page_rank_entry = page_rank_db.get_item(doc_id=1)
page_rank_entry_id = page_rank_entry['doc_id']
# It is a string, need to convert it to float
page_rank_entry_score = float(page_rank_entry['score'])
print "doc id: ",page_rank_entry_id, " ", "score: ", str(page_rank_entry_score)  

""" Get Document Index Item """
document_index_entry = document_index_db.get_item(doc_id=1)
document_index_entry_id = document_index_entry['doc_id']
document_index_entry_url = document_index_entry['url']
print "doc id: ",document_index_entry_id, " ", "url: ", document_index_entry_url
'''

