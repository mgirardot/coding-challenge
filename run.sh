#!/usr/bin/env bash
# the input directory tweet_input and output the files in the directory tweet_output
python ./src/main/tweet_cleaning/tweet_cleaning.py ./tweet_input/tweets.txt ./tweet_output/ft1.txt
python ./src/main/rolling_average/rolling_average.py ./tweet_input/tweets.txt ./tweet_output/ft2.txt



