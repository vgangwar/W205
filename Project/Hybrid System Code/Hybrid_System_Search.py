'''
This is the search program that returns logs just for or condition
it will take input in the following format
python query.py chore out till
'''
import sys
import pymongo
from pymongo import MongoClient
from boto.s3.key import Key
from boto.s3.connection import S3Connection
mc=MongoClient('ec2-52-0-148-244.compute-1.amazonaws.com',27017)
dbmc=mc.invertIndex
conn=S3Connection('AKIAIYLOTPF2BLEYF7PQ','mlfdyDjUQoZgXqrbuOH75Ti/cPPXUxJzn+BLSs2r')
buck=conn.get_bucket('srs-mids-anant5')
k=Key(buck)
query1=dict({"$match":{"$or":None}})
query2=dict({"$unwind":'$arr'})
#query3= dict({"$group":{"_id":"null","arr":{"$addToSet":"$arr"}}})
#"arr":{"$addToSet":"$arr"}
query3=dict({"$group":{"_id":{},"arr":{"$addToSet":"$arr"}}})
i=0
l=list([])
while i<  (len(sys.argv)-1):

    l.append({'word': sys.argv[i+1]})
    i+=1          

query1["$match"]["$or"]=l
cur=dbmc.invertedIndex.aggregate([query1,query2,query3])
#k.key='10001'
#print k.get_contents_as_string()

strx=None
for doc in cur:
    
    y= list(doc['arr'])
    
    for i in y:
        
        k.key=str(i)
        try:
            print(k.get_contents_as_string())
            k.close()
        except Exception:
            print "waste"
            

              
