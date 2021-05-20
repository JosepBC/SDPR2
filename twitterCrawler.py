#!/usr/bin/env python3
import tweepy, json, os, time
from tweepy.parsers import JSONParser
from lithops.storage.cloud_proxy import open
from dotenv import load_dotenv

vaccine = "Moderna"

if __name__ == "__main__":
    load_dotenv()
    auth = tweepy.OAuthHandler(os.environ.get("consumer_key"), os.environ.get("consumer_secret"))
    auth.set_access_token(os.environ.get("access_token"), os.environ.get("access_secret"))

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, parser=JSONParser(), retry_count=5, retry_delay=5)

    maxId = -1
    tweetCount = 0
    tweetsPerQry = 100
    maxTweets = 1000000
    c=0
    while tweetCount < maxTweets:
        if(maxId <= 0):
            newTweets = api.search(q=vaccine, lang="es", count=tweetsPerQry, result_type="mixed", tweet_mode="extended")
        else:
            newTweets = api.search(q=vaccine, lang="es", count=tweetsPerQry, max_id=str(maxId - 1), result_type="mixed", tweet_mode="extended")

        if not newTweets:
            break
        
        for tweet in newTweets["statuses"]:
            with open("RawData/"+vaccine+"/"+tweet["id_str"]+".json", mode='w') as jsonf:
                json.dump(tweet, jsonf, indent=4)


        tweetCount += len(newTweets)
        maxId = newTweets["statuses"][-1]["id"]
        print(maxId)
        c+=1
        if (c % 100 == 0):
            print("Going to sleep")
            time.sleep(5)