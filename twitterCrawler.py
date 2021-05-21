#!/usr/bin/env python3
import tweepy, json, os, time
from tweepy.parsers import JSONParser
from lithops.storage.cloud_proxy import open
from dotenv import load_dotenv
import sys

if __name__ == "__main__":
    maxId = -1
    tweetCount = 0
    tweetsPerQry = 100
    maxTweets = 1000000
    count = 0

    if(len(sys.argv) == 1):
        print("Format: twitterCrawler.py vaccine query max_id")

    if(len(sys.argv) < 2):
        print("Error, you must specify the vaccine")
        exit
    
    vaccine = sys.argv[1]
    print("Vaccine: "+str(vaccine))
    
    if(len(sys.argv) < 3):
        query = vaccine
    else:
        query = sys.argv[2]

    print("Query: "+query)

    load_dotenv()
    auth = tweepy.OAuthHandler(os.environ.get("consumer_key"), os.environ.get("consumer_secret"))
    auth.set_access_token(os.environ.get("access_token"), os.environ.get("access_secret"))

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, parser=JSONParser(), retry_count=5, retry_delay=5)

    if(len(sys.argv) > 3):
        maxID = sys.argv[3]
        print("Using max_id: "+str(maxID))

    while tweetCount < maxTweets:
        if(maxId <= 0):
            newTweets = api.search(q=query, lang="es", count=tweetsPerQry, result_type="mixed", tweet_mode="extended")
        else:
            newTweets = api.search(q=query, lang="es", count=tweetsPerQry, max_id=str(maxId - 1), result_type="mixed", tweet_mode="extended")

        if(len(newTweets["statuses"]) == 0):
            print("No more tweets")
            break
        
        for tweet in newTweets["statuses"]:
            with open("RawData/"+vaccine+"/"+tweet["id_str"]+".json", mode='w') as jsonf:
                json.dump(tweet, jsonf, indent=4)

        tweetCount += len(newTweets["statuses"])
        print("Tweets: "+str(tweetCount))
        maxId = newTweets["statuses"][-1]["id"]
        print("Max id: "+str(maxId))
        count += 1
        if not (count % 10):
            print("Going to sleep")
            time.sleep(5)