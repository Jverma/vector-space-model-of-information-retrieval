import MapReduce
import sys
from collections import *
from math import *

"""
Length of tf-idf vectors in the Simple Python MapReduce Framework.
Takes inverted index as input and spits length of vectors. These will be used to
compute cosine similarity
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record : [term, count, [doc_id, tf_idf]]
    # key: word
    # value: tf_idf
    word = record[0]
    count = record[1]
    docs = record[2]
    for x in docs:
        mr.emit_intermediate(x[0], x[1])
        #print x

def reducer(key, list_of_values):
    # key: doc_id
    # value: list of tf-idf scores for the terms
    sum = 0
    for x in list_of_values:
        square = x**2
        sum = sum + square
    length = sqrt(sum)    
    mr.emit((key, length))

# output = [doc_id, length of tf-idf vector for the doc]
# =============================
apple = []
if __name__ == '__main__': 
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
  
  
