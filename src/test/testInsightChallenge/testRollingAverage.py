'''
Created on 23 nov. 2015

@author: mgirardot
'''
import unittest
from src.main.rolling_average.rolling_average import readCleanTweets
from src.main.rolling_average.rolling_average import computeRollAverage

class Test(unittest.TestCase):
    
    def setUP(self):
        pass
    
    def testSortReadCleanTweets(self):
        text = ["New and improved #HBase connector for #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)", "#Hadoop and #Spark (timestamp: Thu Oct 29 17:51:30 +0000 2015)"]
        result = readCleanTweets()
        result.tweets = text

        self.assertEqual(result.sort(), ([1446141090,1446141119], [["Hadoop","Spark"],["HBase", "Spark"]]))
        
    def testRollAvg(self):
        timings = [01,30,55,56,59,65]
        tag_list = [["Spark","Apache"],["Apache","Hadoop", "Storm"],["Apache"],["Flink","Spark"], ["HBase","Spark"], ["Hadoop","Apache"]]
        result = computeRollAverage()
        
        self.assertEqual(result.rollAvg(timings, tag_list), [2.00,2.00,1.67,1.67,1.67,1.67])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testReadCleanTweets']
    unittest.main()
    