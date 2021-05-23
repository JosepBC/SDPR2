#!/usr/bin/env python3
from lithops.executors import FunctionExecutor
from lithops.multiprocessing import Pool
from lithops.storage.cloud_proxy import os, open
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dateutil import parser
import json, csv, sys

def ls(path):
    paths = []
    for root, _, files in os.walk(path):
        for file in files:
            paths.append(root+"/"+file)
    return paths

def get_tweet_sentiment(tweetText):
    sia = SentimentIntensityAnalyzer()
    return sia.polarity_scores(tweetText)['compound']

def process_tweet(tweetPath):
    with open(tweetPath, mode="r") as f:
        tweet = json.load(f)
    vaccine = tweetPath.split("/")[1]
    id_str = tweet["id_str"]
    date = str(parser.parse(tweet["created_at"]).date())
    userLoc = tweet["user"]["location"]
    sentiment = "0"#get_tweet_sentiment(tweet["full_text"])
    
    #with open("PreProcessed/"+vaccine+"/"+id_str+".csv", mode = 'w') as csvf:
    #    outF = csv.writer(csvf, delimiter = ";")
    #    outF.writerow((vaccine, id_str, date, userLoc, sentiment))

    return
    #return {"vaccine": vaccine, "id_str": id_str, "date": date, "userLoc": userLoc, "sentiment": sentiment}

def group_by_vaccine(results):
    grouped = {"Pfizer":[], "Janssen":[], "Moderna":[], "astrazeneca":[], "sputnik": []}

    for val in results:
        grouped[val["vaccine"]].append(val)
        
    return grouped

def write_cos(vaccine):
    paths = ls("PreProcessed/"+vaccine)
    with open("Processed/"+vaccine+".csv", mode = "w") as csvFile:
        for path in paths:
            with open(path, mode = "r") as srcF:
                csvFile.write(srcF.read())

    return True

def join_paths(results):
    joined = []
    for groupPaths in results:
        for path in groupPaths:
            joined.append(path)
    return joined

if __name__ == "__main__":
    vaccinePathList = ["RawData/Pfizer", "RawData/Janssen", "RawData/Moderna", "RawData/astrazeneca", "RawData/sputnik"]
    with Pool() as pool:
        #Paralel ls over each vaccine folder of the bucket
        print("*****************************")
        print("Starting ls")
        print("*****************************")
        allPaths = pool.map(ls, vaccinePathList)

    with FunctionExecutor() as fexec:
        for groupPath in allPaths:
            for path in groupPath:
                fexec.call_async(process_tweet, path)
        
        print(fexec.get_result())

    #with Pool() as pool:
    #    #Process each file individually in diferent threads
    #    print("*****************************")
    #    print("Starting to process tweets")
    #    print("*****************************")
    #    for groupPath in allPaths:
    #        res = pool.map(process_tweet, groupPath)
    #    
    #    print(res)
    
    """
    print("*****************************")
    print("Starting to write csv")
    print("*****************************")
    vaccineList = ["Pfizer", "Janssen", "Moderna", "astrazeneca", "sputnik"]
    with FunctionExecutor() as fexec:
        #Write each vaccine elems in one csv, one thread per vaccine
        for vaccine in vaccineList:
            fexec.call_async(write_cos, vaccine)
        
        print(fexec.get_result())
        """