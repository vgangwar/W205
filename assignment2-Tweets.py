import sys
import tweepy
import datetime
import urllib
import signal
import json
import nltk
import os

def convertToList(mostCm):
    myList = list()
    for i in mostCm:
        for c in range(i[1]):
            myList.append(i[0])
    return myList

# Function to display histogram of word counts using nltk module
def displayhist():
    myPath = os.getcwd()
    totalContent = str()
    for f in os.listdir(myPath):
        if '.txt' in f:
            fhndl = open(f, "r")
            content = fhndl.read() # Reading file containing tweets
            totalContent = totalContent + content # Merging tweet files to create a single file
            fhndl.close()
    myList = totalContent.split() # Tokenzing tweets
    fd = nltk.FreqDist([w.lower() for w in myList if w.isalpha()])
    fdMostCommon = nltk.FreqDist(convertToList(fd.most_common(50)))
    fdMostCommon.plot() # Creating histogram

def interrupt(signum, frame):
    print "Control-C interrupt"
    ts.flushtoDisk()
    exit(1)

# This class stores ._tweets_per_file in tweets in memory and then flushes into disk
class TweetSerializer:
    def __init__(self):
        self._fileCounter = 0
        self._tweets_per_file = 1000
        self._tweetList = list() # LIst structure that holds tweets in memory

    def flushtoDisk(self): # This function flushes the list ._tweetList to disk
        fname = "tweets_" + str(self._fileCounter) + ".txt"
        self._filehndl = open(fname, "w")
        for x in range(0, len(self._tweetList)):
            self._filehndl.write(self._tweetList[x]); self._filehndl.write("\n")
        self._filehndl.close()
        
    def checkFlush(self): # Checks if tweets can be flushed to disk
        if len(self._tweetList) == self._tweets_per_file:
            self.flushtoDisk()
            self._tweetList = list()
            self._fileCounter += 1
            
    def writeTweet(self, tweet): # Writes tweets into memory before checking if tweets can be flushed to disk
        self.checkFlush()
        self._tweetList.append(tweet)
        
def main():
    signal.signal(signal.SIGINT, interrupt)
    global ts
    ts = TweetSerializer()
    
    consumer_key = "ypUGeCG3CM9pppDU6OCifbr24";
    consumer_secret = "wbJLRa0Y0MgRRfh7IRmKdnzZwW5UQMaY1Hn6iYbjzOzVDKSBgn";
    access_token = "2507649540-Z69OGKwQDsf7lp8tL43W247c41qBinkd1N76Yr1";
    access_token_secret = "9UcHLzHbUCtqqq8I2TAE4EbntG1352J7hHyqSN4Kb9xxH";

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

    searchstr = "#microsoft OR #majong" # Search string
    q = urllib.quote(searchstr) # URL encoded query

    for tweet in tweepy.Cursor(api.search,q=q, since = '2015-02-01', until = '2015-02-08').items():
        text = tweet._json['text'].encode('utf8')
        ts.writeTweet(text)
    ts.flushtoDisk()

    displayhist() # Generate histogram
    
if __name__ == '__main__':
	main()
