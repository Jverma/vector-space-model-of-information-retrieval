import sys
import json
from collections import *
from math import *
import operator

"""
Returns the documents ordered by their relevance to a query.
Takes the inverted index and length vectors as input (which are computed offline).
Submit a query and the script computes the cosine similarity of tf-idf vectors of all documents
with the query vector. 
"""

#creates a dictionary of doc_ids and lengths and an empty dictionary of cosine scores
length_vectors = {}                     
#open the json file containg lengths of tf-idf vectors of all documents
# line = [doc_id, length_of_vector]
cosine_similarity = {}
doc_lengths = open("vector_length.txt")
for line in doc_lengths:
    data = json.loads(line)
    doc_id = data[0]
    length = data[1]
    length_vectors[doc_id] = length
    cosine_similarity[doc_id] = 0



#creates a dictionary of words and the [doc, tf-idf]
Index = {}
#open the inverted index
# line = [word, [doc_id, tf-idf]]
inverted_index = open("index.txt")
for line in inverted_index:
    data = json.loads(line)
    word =  data[0]
    count = data[1]
    docs = data[2]
    Index[word] = docs


#computes the cosine similarity
def relevance(query):
    similarity = {}
    query_vector = query.split(' ')
    for x in query_vector:
        relevant_docs = Index[x]
        for d in relevant_docs:
            document = d[0]
            score = d[1]
            cosine_similarity[document] = cosine_similarity[document] + score
    for y in cosine_similarity.keys():
        cosine_simiarity[y] = float(cosine_similarity[y])/float(len(query_vector)*length_vectors[y])
    sorted_similarity = sorted(cosine_similarity.iteritems(), key=operator.itemgetter(1), reverse=True)    
    return sorted_similarity

#Enter query
search_query = sys.argv[1]
print relevance(search_query)
