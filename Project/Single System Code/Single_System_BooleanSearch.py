from pymongo import Connection
import sys

class BooleanSearch:
'''
This class accepts a word, queries MongoDB and returns all the postings list as a Python set
Also, given two sets of postings list this class performs boolean operations of AND, OR and NOT using 
python set object methods of Intersection, Union and Difference.
Additionally, given a set of postings list, it queries the chunk files to ouput the log entries
'''
    def __init__(self):
        self.con = Connection()
        self.db = self.con.indexer
        self.idf = self.db.idf
        self.outputPath = "Processed_Data/"

    def __del__(self):
        self.con.close()

    def getPostingsListSet(self, word): # Given a word returns a set of postings list from MongoDB 
        mySet = set()
        for row in self.idf.find({'Term':word}):
            pList = row['postingsList']
            mystr = '.'
            mystr = mystr.join([str(x) for x in pList.values()])
            mySet.add(mystr)
        return mySet

    def printLogs(self, pListSet): # Give a set of postings list, queries the chunk files to output the logs
        for oneList in pListSet:
            items = oneList.split('.')
            fn = self.outputPath + items[0]
            fhndl = open(fn, 'r')
            fhndl.seek(int(items[1]))
            print fhndl.read(int(items[2])).strip()
            fhndl.close()

    def termAND(self, set1, set2): # Given 2 sets of postings list, performs boolean AND
        setAND = set1.intersection(set2)
        return setAND

    def termOR(self, set1, set2): # Given 2 sets of postings list, performs boolean OR
        setOR = set1.union(set2)
        return setOR

    def termNOT(self, set1, set2): # Given 2 sets of postings list, performs boolean NOT
        setNOT = set2.difference(set1)
        return setNOT

    def singleTerm(self, word): # returns postings list for a single term when no boolean operations is required
        return self.getPostingsListSet(word)

class MyStack:
'''
Implements a stack data structure for query processing. It is used both during infix to postfix conversion and 
during postfix processing
'''
    def __init__(self):
        self._stack = list()

    def push(self, item):
        self._stack.append(item)

    def peek(self):
        if len(self._stack) > 0:
            return self._stack[-1]
        else:
            return False

    def pop(self):
        if len(self._stack) > 0:
            return self._stack.pop()
        else:
            return False
        
    def showStack(self):
        print self._stack

class Infix2Postfix:
'''
Uses shunting yard algorithm to convert infix to postfix notation. At same time it replaces each term in the postfix 
notation with it's set of postings list
'''
    def __init__(self, infix):
        self._infix = infix.split()
        self._postfix = list()
        self._myStack = MyStack()
        self._bsearch = BooleanSearch()

    def getPostfix(self):
        self.convert2Postfix(self._infix)
        return self._postfix

    def convert2Postfix(self, infix): # Converts infix to postfix and replaces terms with their respective set of postings list
        for c in range(len(infix)):
            if infix[c] not in ['&', '|', '!', '(', ')']:
                self._postfix.append(self._bsearch.getPostingsListSet(infix[c]))
            elif infix[c] == ')':
                operator = self._myStack.pop()
                while (operator != '('):
                    self._postfix.append(operator)
                    operator = self._myStack.pop()
            else:
                self._myStack.push(infix[c])
        operator = self._myStack.pop()
        while (operator != False):
            self._postfix.append(operator)
            operator = self._myStack.pop()

class SolvePostfix:
'''
Processes postfix notation to generate the final postings list set and returns the result to user
'''
    def __init__(self, pstfx):
        self._postfix = pstfx
        self._result = set() # Used to store the final postings list set that satisfies the user query
        self._myStack = MyStack()
        self._bsearch = BooleanSearch()

    def getResult(self): # returns the log entries satisfying the query
        self.processResult(self._postfix)
        self._bsearch.printLogs(self._result)

    def getLogCount(self): # returns the count satisfying the query
        self.processResult(self._postfix)
        return len(self._result)

    def processResult(self, pstfx): # Uses stack data structure to solve the postfix notation
        for c in range(len(pstfx)):
            if pstfx[c] not in ['&', '|', '!', '(', ')']:
                self._myStack.push(pstfx[c])
            else:
                if pstfx[c] == '&':
                    result = self._bsearch.termAND(self._myStack.pop(), self._myStack.pop())
                    self._myStack.push(result)
                elif pstfx[c] == '|':
                    result = self._bsearch.termOR(self._myStack.pop(), self._myStack.pop())
                    self._myStack.push(result)
                else:
                    result = self._bsearch.termNOT(self._myStack.pop(), self._myStack.pop())
                    self._myStack.push(result)
        self._result = self._myStack.pop()

def main():
    query = sys.argv[1] # Accepts query as a command line argument
    infix2pstfx = Infix2Postfix(query)
    sp = SolvePostfix(infix2pstfx.getPostfix())
    sp.getResult()		# This method returns the actual logs that satisfy the user query
    print sp.getLogCount()	# This method returns the count of the logs that satisfy the user query

# Standard boilerplate to call the main() function.
if __name__ == '__main__':
    main()
