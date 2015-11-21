'''
@author: mgirardot

'''
import json
import unicodedata
import sys
import os

class tweetParser():
    ''' A parser for tweets in a text file.
    Needs a file name as an argument.'''
    
    def __init__(self,filename):
        self.filename = filename
        self.tweets_data = []
        
    def parse(self):
        with open (self.filename, 'r') as tweet_file:
            for line in tweet_file:
                try:
                    tweet = json.loads(line)
                    self.tweets_data.append(tweet)
                except:
                    continue
    
    def get_tweet_list(self):
        return self.tweets_data   

class tweetCleaner():
    '''A class to handle unicode charachters in the text of tweets.
    Needs a list of tweets as argument. '''
    
    def __init__(self,tweets_data):
        self.tweets_data = tweets_data
        self.text_file = []
        
    def clean(self):        
        for tweet in self.tweets_data:
            #remove unicode and replace by equivalent ascii
            t = unicodedata.normalize('NFKD', tweet['text']).encode('ascii','ignore')
            #remove \n char inside the text
            t = "".join(t.split('\n'))
            
            #convert time from unicode to ascii
            d = unicodedata.normalize('NFKD', tweet['created_at']).encode('ascii','ignore')
            
            #assemble the tweet into one line and append to a list
            self.text_file.append("" +  t + " (timestamp: " + d + ")\n")

    def write(self,out_filename):
        with open(out_filename, 'ab') as f:
            for line in self.text_file:
                try:
                    f.write(line)
                except:
                    continue
                
if __name__ == "__main__":
    inputFile = os.path.realpath(sys.argv[1])
    outputFile = os.path.realpath(sys.argv[2])
    #reading
    tp = tweetParser(inputFile)
    tp.parse()
    list_tweet = tp.get_tweet_list()
    
    #cleaning
    tc = tweetCleaner(list_tweet)
    tc.clean()
    
    #writing
    tc.write(outputFile)
    