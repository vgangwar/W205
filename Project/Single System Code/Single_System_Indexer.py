from pymongo import Connection
import pymongo
import os

class DiskLogWriter:
'''
This class receives a single log entry at a time. It writes the log into a chunk file 
and returns the postings list. Postings list = Filename, offset, logLength 
It also manages the size of the chunk file. It creates a new file if the size of the chunk 
crosses the threshod of 1MB
'''
    def __init__(self, outputPath):
        self.outputPath = outputPath
        self.currentfilename = "0"
        self.FileLengthLimit = 1048576       # 1MB in bytes
        self.currentFilePosition = 0
        self.currentFilehandle = self.manageOutputFile("FirstRun")

    def writeLogtoDisk(self, logEntry):
        self.manageOutputFile(logEntry)
        pList = {'documentName':self.currentfilename, 'currentFilePosition':self.currentFilePosition, 'length':len(logEntry)}
        self.currentFilehandle.write(logEntry)
        self.currentFilePosition = self.currentFilePosition + len(logEntry)
        return pList

    def manageOutputFile(self, logEntry):
        if logEntry == 'FirstRun':
            fn = self.outputPath  + self.currentfilename
            return open(fn, 'w')
        if len(logEntry) + self.currentFilePosition > self.FileLengthLimit:
            self.currentFilehandle.close()
            fn = int(self.currentfilename) + 1
            self.currentfilename = str(fn)
            self.currentFilePosition = 0
            fn = self.outputPath  + self.currentfilename
            self.currentFilehandle = open(fn, 'w')

    def showState(self):
        print "self.currentfilename:", self.currentfilename
        print "self.FileLengthLimit:", self.FileLengthLimit
        print "self.currentFilePosition:", self.currentFilePosition
        print "self.currentFilehandle:", self.currentFilehandle

    def __del__(self):
        self.currentFilehandle.close()

class Indexer:
'''
This class splits each log into words. Then removes duplicates. It then sends the 
log to the class DiskLogWriter() to obtain the postings list. It then stores the word and the postings list into MongoDB.
'''
    def __init__(self, writer):
        self.writer = writer
        self.con = Connection()
        self.db = self.con.indexer
        self.idf = self.db.idf
        self.idf.remove()
        self._bulkLimit = 10000
        self._counter = 0
        self.initializeBulk()

    def __del__(self):
        self.idf.create_index('Term')
        self.con.close()

    def initializeBulk(self):
        self.bulk = self.idf.initialize_unordered_bulk_op()

    def bulkInsertAdd(self, log):
        pList = self.writer.writeLogtoDisk(log)
        self._counter += 1
        wordSet = set(log.split())	# Converting to set to remove duplicates
        for word in wordSet:
            self.bulk.insert({'Term': word, 'postingsList': pList})
            if self._counter > self._bulkLimit:
                self.bulkExecute()
                self.initializeBulk()
                self._counter = 0

    def bulkExecute(self):
        try:
            self.bulk.execute()
        except pymongo.errors.BulkWriteError as bwe:
            print "Caught duplicate"
            pass

def main():
    inputPath = 'Input_Data/'		# Path to read input log files from
    outputPath = 'Processed_Data/'	# Path to store chunk files in
    writer = DiskLogWriter(outputPath)
    myIndexer = Indexer(writer) 
    for filename in os.listdir(inputPath): # This loops reads all the the input logs
        filename = inputPath + filename
        fhndl = open(filename, 'r')
        for line in fhndl.readlines():	# This loop splits each input log into lines and call the Indexer class
            myIndexer.bulkInsertAdd(line)

if __name__ == '__main__':
	main()
