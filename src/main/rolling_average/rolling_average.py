'''
Created on 23 nov. 2015

@author: mgirardot
'''
import re
import datetime as dt
import sys
import os

class readCleanTweets():
    def __init__(self):
        
        self.tweets = []
        self.sortedTimeSerie = []
        self.timings =[]
        self.tag_list = []
        self.TIME_RE = re.compile("timestamp: (?P<weekday>\w+) (?P<month>\w+) (?P<monthday>\w+) (?P<hour>\d+):(?P<min>\d+):(?P<sec>\d+) \+\w+ (?P<year>\d+)")
        self.TAG_RE = re.compile("#(\w+)")
        
        #read the file
    def read(self, filename):
        self.filename = filename
        with open(self.filename, 'r') as tweet_file:
            for line in tweet_file:
                try:
                    self.tweets.append(line)
                except:
                    continue
    
    def sort(self):
        #match patterns with regular expressions
        for line in self.tweets:
            #convert the timestamp into a total seconds timestamp
            m = self.TIME_RE.search(line)
            strtime = ''+ m.group('year')+' '+m.group('month')+' '+m.group('monthday')+' '+ m.group('hour')+' '+ m.group('min')+' '+m.group('sec')
            
            formatted_time = dt.datetime.strptime(strtime, '%Y %b %d %H %M %S')
            
            timestamp = (formatted_time - dt.datetime(1970,1,1)).total_seconds()
            
            #get the list of tags
            tags = self.TAG_RE.findall(line)
            
            #collect timestamp and tags into a sortable list
            if len(tags)>1:
                self.sortedTimeSerie.append([int(timestamp), tags])
            
        #sort the list and split into two lists
        self.sortedTimeSerie.sort()
        for element in self.sortedTimeSerie:
            self.timings.append(element[0])
            self.tag_list.append(element[1])
    
        return (self.timings, self.tag_list)

class computeRollAverage():
    def __init__ (self):
        self.rollAverage = []
    
    def rollAvg(self,timings, tag_list, windowLength = 60):  
        self.timings = timings
        self.tag_list = tag_list
        self.starTime = self.timings[0]  
        self.windowLength = windowLength
        self.window = self.starTime + self.windowLength-1
        
        itags = []
        tags = {}
        
        #compute the first window
        for i in range(self.starTime, self.window):
            try:
                itags = self.tag_list[self.timings.index(i)] #0-59
                if len(itags) > 1:
                    for t in itags:
                        if (t in tags.keys()):
                            tags[t] += len(itags)-1  #degree of the node incremented by the number of edges
                        else:
                            tags[t] = len(itags)-1   # or created if not in the dict
                    
            except ValueError:
                pass
         
        total_links = float(reduce(lambda x,y: x+y, tags.values()))
        total_tags = float(len(tags))
    
        self.rollAverage.append(round(total_links/total_tags, 2))
       
        #compute the following windows
        #remove out-of-range tags
        for i in range(self.window, self.timings[-1]): #from the end of the second window to the end
            try:
                #select the tags to throw away
                toThrow = self.tag_list[self.timings.index(i - self.windowLength)] 
                
                #remove each tag from the dict
                for t in toThrow:
                    if (tags[t] > len(itags)-1):
                        tags[t] -= len(itags)-1
                    else:
                        del tags[t]
                    
            except ValueError:
                pass
            
            try:
                #once the trailing tags are removed add the new tags from the new window
                itags = self.tag_list[self.timings.index(i)]
                if len(itags) > 1:
                    for t in itags:
                        if (t in tags.keys()):
                            tags[t] += len(itags)-1  #degree of the node incremented by the number of edges
                        else:
                            tags[t] = len(itags)-1   # or created if not in the dict
                
            except ValueError:
                pass
            
            total_links = float(reduce(lambda x,y: x+y, tags.values()))
            total_tags = float(len(tags))
            self.rollAverage.append(round(total_links/total_tags, 2))
            
        return self.rollAverage
    
    def write(self,out_filename):
        with open(out_filename, 'w') as f:
            for line in self.rollAverage:
                #print("\nprinting.....{}".format(line))
                try:
                    f.write('%.2f\n' % line)
                except:
                    continue

if __name__ == '__main__':
    inputFile = os.path.realpath(sys.argv[1])
    outputFile = os.path.realpath(sys.argv[2])
    print("input: {}" .format(inputFile))
    print("output: {}" .format(outputFile))
    
    
    #reading
    tp = readCleanTweets()
    tp.read(inputFile)
    
    #print("tweets: {}" .format(tp.tweets))
    
    #get the timestamps and tag list in sorted order
    timings,tag_list = tp.sort()
    #print("\ntimings: {}" .format(tp.timings))
    #print("\ntags: {}" .format(tp.tag_list))
    
    #instance of computeRollAverage
    ra = computeRollAverage()
    
    result = ra.rollAvg(timings,tag_list)
    #print the begining of the results
    print("\nResults:\n")
    print(result[:5])
    
    #write the result in a text file
    ra.write(outputFile)
    
    
    