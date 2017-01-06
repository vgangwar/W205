import sqlite3
import os
import json
import re

class sqliteWork:
    def __init__(self):
        self._db = sqlite3.connect('sqliteDB')
	self._cursor = self._db.cursor()
	self.createTable()

    def createTable(self):
	self._cursor.execute('''CREATE TABLE tweets(tweetKey INTEGER UNIQUE, username TEXT, datetimeStamp TEXT)''')
	self._cursor.execute('''CREATE TABLE hashtags(tweetKey INTEGER, hashtag TEXT, datetimeStamp TEXT)''')
	self._db.commit()

    def insertRecord(self, tweetFilesPath):
        for dictValues in self.readTweetFiles(tweetFilesPath):
	    # Inserting tweets
	    self._cursor.execute('''INSERT INTO tweets(tweetKey, username, datetimeStamp) VALUES(?,?,?)''', dictValues['tweet'])
	    # Inserting hashtags
	    tweetKey = dictValues['hashtags'][0]
	    hashtagsList = dictValues['hashtags'][1]
	    timestamp = dictValues['hashtags'][2]
	    for ht in hashtagsList:
		htTuple = (tweetKey, ht, timestamp)
		self._cursor.execute('''INSERT INTO hashtags(tweetKey, hashtag, datetimeStamp) VALUES(?,?,?)''', htTuple)
	self._db.commit()

    def readTweetFiles(self, tweetFilesPath):
        tweetFiles = os.listdir(tweetFilesPath)
        tweets = list()
        for fileName in tweetFiles:
            fhndl = open(tweetFilesPath + fileName, 'r')
            tweets = tweets + json.load(fhndl)
            fhndl.close()
        for c in range(len(tweets)):
            # Creating tuple for tweets table
	    tweetKey = c
            username = tweets[c]['user']['screen_name']
            timestamp = tweets[c]['created_at']
	    timestamp = self.sqliteTimeString(timestamp)
            tweetTuple = (tweetKey, username, timestamp)
            # List for hashtags table
            hashtagsList = [d['text'].encode('ascii') for d in tweets[c]['entities']['hashtags']]
	    hashtagsTuple = (tweetKey, hashtagsList, timestamp)
            yield {'tweet': tweetTuple, 'hashtags': hashtagsTuple}

    def getRecord(self, tableName):
	query = 'select * from {0}'.format(tableName)
	print query
	self._cursor.execute(query)
	all_rows = self._cursor.fetchall()
	for row in all_rows:
	    print row

    def mostTweeted(self):
	clauseDay1 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-14 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-14 15:00:00'))"
        clauseDay2 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-15 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-15 15:00:00'))"
	query = "SELECT username, count(username) as c from tweets where {0} OR {1} group by username order by c desc limit 1".format(clauseDay1, clauseDay2)
        self._cursor.execute(query)
        all_rows = self._cursor.fetchall()
	print "Mosted Tweeted"
        for row in all_rows:
            print row[0] + "; Number of tweets:" + str(row[1])

    def top10Hashtags(self):
	clauseDay1 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-14 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-14 15:00:00'))"
        clauseDay2 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-15 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-15 15:00:00'))"
        query = "SELECT hashtag, count(hashtag) as c from hashtags where {0} OR {1} group by hashtag order by c desc limit 10".format(clauseDay1, clauseDay2)
        self._cursor.execute(query)
        all_rows = self._cursor.fetchall()
	print "Top 10 Hashtags"
        for row in all_rows:
            print row[0] + "; Number of tweets:" + str(row[1])

    def tweetsPerHour(self):
	clauseDay1 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-14 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-14 15:00:00'))"
        clauseDay2 = "(strftime('%s',datetimeStamp) >= strftime('%s','2015-02-15 08:00:00') AND strftime('%s',datetimeStamp) <= strftime('%s','2015-02-15 15:00:00'))"
        query = "SELECT strftime('%Y-%m-%d %H', datetimeStamp) as date_hr, count(strftime('%Y-%m-%d %H', datetimeStamp)) from tweets where {0} OR {1} group by date_hr".format(clauseDay1, clauseDay2)
        self._cursor.execute(query)
        all_rows = self._cursor.fetchall()
	print "Tweets per Hour"
        for row in all_rows:
            print row[0] + "; Number of tweets:" + str(row[1])

    def sqliteTimeString(self, datetime):
        months = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}
        retval = re.search(r'(\w+) (\w+) (\w+) (\w+:\w+:\w+) \+(\w+) (\w+)', datetime)
        converted = retval.group(6) + '-' + months[retval.group(2)] + '-' + retval.group(3) + 'T' + retval.group(4)
        return converted

    def __del__(self):
	self._cursor.execute('''DROP TABLE tweets''')
	self._cursor.execute('''DROP TABLE hashtags''')
	self._db.commit()
        self._db.close()

def main():
	tweetFilesPath = 'tweetFiles/'
	sqliteWorker = sqliteWork()
	sqliteWorker.insertRecord(tweetFilesPath)
	#sqliteWorker.getRecord('tweets')
	#sqliteWorker.getRecord('hashtags')
	sqliteWorker.mostTweeted()
	sqliteWorker.top10Hashtags()
	sqliteWorker.tweetsPerHour()

if __name__ == '__main__':
	main()

