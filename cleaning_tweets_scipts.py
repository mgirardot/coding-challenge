# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 10:34:19 2015

@author: mgirardot
"""

import json

tweet_data_path = "C:/Users/mgirardot/Desktop/twitter-coding-challenge/coding-challenge/tweet_input/tweets.txt"


tweets_data = []
with open(tweet_data_path,'r') as tweet_file:
    for line in tweet_file:
        try:
            tweet = json.loads(line)
            tweets_data.append(tweet)
        except:
            continue


import unicodedata

text_file = []
for tweet in tweets_data:
    t = unicodedata.normalize('NFKD', tweet['text']).encode('ascii', 'ignore')
    t = "".join(t.split('\n'))
    d = unicodedata.normalize('NFKD', tweet['created_at']).encode('ascii', 'ignore')
    text_file.append("" + t + " (timestamp: " + d + ")\n")

out_tweet = "C:/Users/mgirardot/Desktop/twitter-coding-challenge/coding-challenge/tweet_output/ft1.txt"

with open(out_tweet, 'ab') as f:
    for line in text_file:
        try:
            f.write(line)
        except:
            continue


import re

HASHTAG = re.compile("#\w+")

i = HASHTAG.findall("j'ai trouvé un #hashtag dans la #phrase.")

for tag in i:
    print(tag)
    
print re.match("Fri", text_file[1])

p = re.compile("timestamp: (\w+)*")
m = p.findall(text_file[1])
print m.group(1)
text_file[1]



##
'''
    get (hash_tag, timestamp, value)
    
    the moving average use an array and simulate queue and enqueue of value
    within a defined period representing the size of the array
    
    for each val:
        add the value to the array
            sum += value
            if (size < period):
                array[pointer++] = value  #store the value if an empty slot is present
                size++
            else: #when the array is full
                pointer = pointer % period  # reset the pointer to the begining of the period
                sum -= window[pointer]  # pop out the value at the begining of the period
                window[pointer++]= value # store the value at the empty pointer position

    average = sum/size


each hash_tag count for one (tag, timestamp)
nb_of_tags = len(set(tags))

for each tweet:
    tags = []
    nb_of_tags = 

sortedTimeSerie = [timestamp,list(tags)].sort()
01,[spark,apache]
30, [apache, hadoop, storm]
55, [apache]  # to remove `` if len(tags)>1:
56,[flink, spark]
59,[hbase, spark]
65, [hadoop, apache]


for i in windowLength-1:  #for 0 to 59
    itags = sortedTimeSerie[sortedTimeSerie[0]==i,1]
    sum += len(itags) # 2+3+2+2
    tags.append(itags)
    
for i in windowlength-1 to len(sortedTimeserie):         # for 59 to 65
    #remove out of range tags
    toThrow = sortedTimeSerie[sortedTimeserie[0] == i-windowLength+1, 1]  # 59-61 = -2 ##nothing
    sum -= len(toThrow) #0
    for t in toThrow:
        tags.remove(t)

    itags = sortedTimeSerie[sortedTimeSerie[0]==i,1]
    sum += sortedTimeSerie[sortedTimeSerie[0]==i,1]         # 9 + 2
    tags.append(itags)
    
    movingaverage = sum/len(set(tags))
    return movingaverage

'''    
#build the sortedTimeSerie
# text #tag1 text #tag2 (time)
import re
import datetime as dt

q = re.compile("timestamp: (?P<weekday>\w+) (?P<month>\w+) (?P<monthday>\w+) (?P<hour>\d+):(?P<min>\d+):(?P<sec>\d+) \+\w+ (?P<year>\d+)")
t = re.compile("#(\w+)")
sortedTimeSerie = []
timings =[]
tag_list = []
for tweet in text_file:
    m = q.search(tweet)
    strtime = ''+ m.group('year')+' '+m.group('month')+' '+m.group('monthday')+' '+ m.group('hour')+' '+ m.group('min')+' '+m.group('sec')
    ln = dt.datetime.strptime(strtime, '%Y %b %d %H %M %S')
    timestamp = (ln - dt.datetime(1970,1,1)).total_seconds()
    
    tags = t.findall(tweet)
    
    if len(tags)>1:
        sortedTimeSerie.append([int(timestamp), tags])
    sortedTimeSerie.sort()
    
for element in sortedTimeSerie:
    timings.append(element[0])
    tag_list.append(element[1])

starTime = timings[0]
sum = 0
tags = []
windowLength = 10
window = starTime+windowLength-1
result_movingAverage = []

for i in range(starTime, window):
    try:
        itags = tag_list[timings.index(i)]
        sum += len(itags)
        tags.append(itags)

    except ValueError:
        pass
    
    
   
for i in range(window, timings[-1]):
    #remove out of range tags
    try:
        toThrow = tag_list[timings.index(i-windowLength)]
        
        sum -= len(toThrow)
        for t in toThrow:
            tags.remove(t)
    except ValueError:
        
        pass
    
    try:
        itags = tag_list[timings.index(i)]
        sum += len(itags)
        tags.append(itags)
    except ValueError:
        pass
    
      
    
    #unlist and convert to set
    flatList = [item for sublist in tags for item in sublist]
    nb_tags = float(len(set(flatList)))
    movingAverage = float(sum)/nb_tags
    
    print(round(movingAverage, 2))
    result_movingAverage.append(round(movingAverage,2))




##########
text = ["New and improved #HBase connector for #Spark (timestamp: Thu Oct 29 17:51:59 +0000 2015)"]
m = q.search(text)
strtime = ''+ m.group('year')+' '+m.group('month')+' '+m.group('monthday')+' '+ m.group('hour')+' '+ m.group('min')+' '+m.group('sec')
ln = dt.datetime.strptime(strtime, '%Y %b %d %H %M %S')
timestamp = (ln - dt.datetime(1970,1,1)).total_seconds()
timestamp
  
for line in text:
    print line

###########################
timings = [01,30,55,56,59,65]
tag_list = [["Spark","Apache"],["Apache","Hadoop", "Storm"],["Apache"],["Flink","Spark"], ["HBase","Spark"], ["Hadoop","Apache"]]
windowLength = 60
window = timings[0] + windowLength-1
tags = {}
rollAvg = []
for i in range(timings[0],window):
    try:
        itags = tag_list[timings.index(i)]
        if len(itags) > 1:
            for t in itags:
                if t in tags:
                    tags[t] += len(itags)-1
                else:
                    tags[t] = len(itags)-1
    except ValueError:
        pass           

total_links = float(reduce(lambda x,y: x+y, tags.values()))
total_tags = float(len(tags))
rollAvg.append(round(total_links/total_tags, 2))
    
for i in range(window, timings[-1]):
    try:
        toThrow = tag_list[timings.index(i - windowLength)]
        for t in toThrow:
            if tags[t] > len(itags)-1:
                tags[t] -= len(itags)-1
            else:
                del tags[t]
    except ValueError:
        pass         

    try:
        itags = tag_list[timings.index(i)]
        if len(itags) > 1:
            for t in itags:
                if t in tags:
                    tags[t] += len(itags)-1
                else:
                    tags[t] = len(itags)-1
    except ValueError:
        pass
    
    total_links = float(reduce(lambda x,y: x+y, tags.values()))
    total_tags = float(len(tags))
    rollAvg.append(round(total_links/total_tags, 2))


###################################################################

def nodeDegree(timings, tag_list, start_time, end_time):
    tags = {}
    itags = []
    for i in range(start_time,end_time):
        try:
            itags = tag_list[timings.index(i)]
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
    return(round(total_links/total_tags, 2))

nodeDegree(timings, tag_list, timings[0], window)

def rollingAverage(timings, tag_list, windowLength):
    window = timings[0] + windowLength-1
    rollAvg = []
    firstWindow = nodeDegree(timings, tag_list, timings[0], window)
    rollAvg.append(firstWindow)
    
    for i in range(window, timings[-1]):
        try:
            toThrow = tag_list[timings.index(i - windowLength)]
        
            for t in toThrow:
                if (tags[t] > len(itags)-1):
                    tags[t] -= len(itags)-1
                else:
                    del tags[t]
        except ValueError:
            pass 
        
        nextWindow = nodeDegree(timings,tag_list, i, i+window)

        rollAvg.append(nextWindow)
    return rollAvg
    
rollingAverage(timings, tag_list, 60)














