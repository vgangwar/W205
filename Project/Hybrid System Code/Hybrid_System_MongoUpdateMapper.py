#!/usr/bin/python
'''
This code will read each line from reducer output and process it in mongodb
'''
import json
import pymongo
import sys
from pymongo import MongoClient
mc=MongoClient('ec2-52-0-148-244.compute-1.amazonaws.com',27017)
dbmc=mc.invertIndex
for line in sys.stdin:
    word=json.loads(line)
    
    dbmc.invertedIndex.update(word[0],word[1],upsert=True)
    
 
    
    
