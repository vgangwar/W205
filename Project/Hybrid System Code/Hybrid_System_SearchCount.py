'''
This program processes input in an or format and returns the count
'''

import sys
import pymongo
from pymongo import MongoClient
from boto.s3.key import Key
from boto.s3.connection import S3Connection
mc=MongoClient('ec2-52-0-148-244.compute-1.amazonaws.com',27017)
dbmc=mc.invertIndex
conn=S3Connection('AKIAIYLOTPF2BLEYF7PQ','mlfdyDjUQoZgXqrbuOH75Ti/cPPXUxJzn+BLSs2r')
buck=conn.get_bucket('srs-mids-anant4')
k=Key(buck)
query1=dict({"word":{"$in":None}})
            
i=0
l=list([])
while i<  (len(sys.argv)-1):

    l.append(sys.argv[i+1])
    i+=1          

query1["word"]["$in"]=l

cur=dbmc.invertedIndex.find(query1)
count=0
for doc in cur:
    count+=1
print count    
 
