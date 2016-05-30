#!/usr/bin/python

'''
The reducer will create the posting for each word. It will ensure that a single line is file is not larger than 10000 bytes.(I did not want to read a very large line into memory for upserting into mongo)
'''
import sys
import json
import sys
#We will go with a max size of X for now
currentWord=None
invertedIndex1={}
invertedIndex2={}
invertedIndex3={"$addToSet":{}}
doc=[]
listSize=0
##print "["
for line in sys.stdin:
     word, doc = line.split('\t', 1)
     #doc= docstr.split('\t', 1)
     if currentWord==word:
          #check size before appending
          if sys.getsizeof(str(invertedIndex2)) < 10000:
               
               invertedIndex2["$each"].append(doc.strip())
          else:
               currentWord=None
               print(json.dumps([invertedIndex1,{"$addToSet":{"arr":invertedIndex2}}]))
               invertedIndex1={}
               invertedIndex2={}
               invertedIndex3={"$addToSet":{}}

               invertedIndex1["word"]=word
               
               
     else:
          if len(invertedIndex2)!= 0:
               print(json.dumps([invertedIndex1,{"$addToSet":{"arr":invertedIndex2}}]))
  
               invertedIndex1={}
               invertedIndex2={}
               invertedIndex3={"$addToSet":{}}

               
          invertedIndex1["word"]=word
          invertedIndex2["$each"]=[doc.strip()]
          currentWord=word

          
     
