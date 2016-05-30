#!/usr/bin/python
'''
The mapper would first get the id prefix from gentable collectoin in table. Thereafter in the for loop it will
take each line of the file as input and encode it to utf-8. Subsequently it will remove stopwords using nltk.
Finally it willstore the line in s3 as a key value and emit each word with its key
'''
import sys
import re
import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
import pymongo
from pymongo import MongoClient

class IndexMapperNew:
    def __init__(self):
        conn=boto.s3.connection.S3Connection('AKIAIYLOTPF2BQ','mlfdyDjUOH75Ti/cPPXUxJzn+BLSs2r')
        self.bucket=conn.get_bucket('srs-mids-anant5')
        
        
    def map(self): 
        mc=MongoClient('ec2-52-0-148-244.compute-1.amazonaws.com',27017)
        dbmc=mc.genid
        idoc=dbmc.gentable.find_one_and_update(filter={},update={ "$inc": { "score": 1 } },upsert=True);
        k=Key(self.bucket)
        y=stopwords.words('english')
        i=1
        strx=str(int(idoc['score']))
        strz=None
        filestring=""
        for line in sys.stdin:
 
            
            line = unicode(line, "utf-8","ignore")
            pattern = re.compile(r'\b(' + r'|'.join(y) + r')\b\s*')
            line = pattern.sub('', line)
            

            tokenizer = RegexpTokenizer(r'\w+')
            words=tokenizer.tokenize(line)
            strz=strx+'a'+str(i)
            k.key=strz
            filestring=line+'\n'
            k.set_contents_from_string(filestring)
            for word in words:
                word=word.encode(encoding='UTF-8',errors='ignore')
                
                print '%s\t%s' % (word.strip(), strz)
            i+=1

                

      
mapper=IndexMapperNew()
mapper.map()

