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

tweets_data[1]['created_at']
tweets_data[3]['text']

import unicodedata
unicodedata.normalize('NFKD', (u"un été a paõ solo")).encode('ascii','ignore')

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


import os
os.path.relpath(os.path.curdir)