#!/usr/bin/env python3
from lithops.executors import FunctionExecutor
from lithops.multiprocessing import Pool
from lithops.storage.cloud_proxy import os, open
import mtranslate
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dateutil import parser
import json, csv

def ls(path):
    paths = []
    for root, _, files in os.walk(path):
        for file in files:
            paths.append(root+"/"+file)
    return paths

def get_tweet_sentiment(tweetText):
    #Translate to spanish using mtranslate
    ready_proces = mtranslate.translate(tweetText, to_language="en")
    #Sentiment analysis
    sia = SentimentIntensityAnalyzer()
    return str(sia.polarity_scores(ready_proces)["compound"])

def make_chunks(path):
    size = 1000
    lst = ls(path)
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def process_tweet(path):
    with open(path, mode = "r", encoding='latin1') as f:
        tweet = json.load(f)
    vaccine = path.split("/")[1]
    id_str = tweet["id_str"]
    date = str(parser.parse(tweet["created_at"]).date())
    userLoc = tweet["user"]["location"]
    geo = str(tweet["geo"])
    sentiment = get_tweet_sentiment(tweet["full_text"])
    
    if geo == "None":
        geo = ""
    return [vaccine, id_str, date, userLoc, geo, sentiment]

def process_chunk(paths):
    out_data = []
    if type(paths) is list:
        vaccine = paths[0].split("/")[1]
        first_file = paths[0].split("/")[2].split(".")[0]
        last_file = paths[-1].split("/")[2].split(".")[0]
        for path in paths:
            out_data.append(process_tweet(path))
    elif type(paths) is str: #If one chunk is of one path
        vaccine = paths.split("/")[1]
        first_file = last_file = paths.split("/")[2].split(".")[0]
        out_data.append(process_tweet(paths))
    else:
        return False

    out_file_name = "ProcesedChunks/"+vaccine+"/"+first_file+"_"+last_file+".csv"            
    with open(out_file_name, mode = "w") as out_file:
        csv_out = csv.writer(out_file)
        csv_out.writerows(out_data)
    

def reduce_csv(results):
    vaccine_name = results[0][0].decode("utf-8").split(",")[0]
    with open("Processed/"+vaccine_name+".csv", mode = "w") as out_f:
        for file in results:
            for line in file:
                out_f.write(line.decode("utf-8")+"\n")

def read_csv(obj):
    return obj.data_stream.read().splitlines()

def write_data(results):
    vaccine = results[0][0][0]
    with open("Procesed/"+vaccine+".csv", mode = "w") as out_f:
        csv_out = csv.writer(out_f)
        csv_out.writerows(results[0])

if __name__ == "__main__":
    vaccine_path_list = ["RawData/sputnik", "RawData/Moderna", "RawData/Pfizer", "RawData/Janssen", "RawData/astrazeneca"]

    print("**************************************************************************")
    print("Making chunks of "+str(vaccine_path_list))
    print("**************************************************************************")
    with Pool() as pool:
        all_paths_chunks = pool.map(make_chunks, vaccine_path_list)

    for vaccine in all_paths_chunks:
        vaccine_name = vaccine[0][0].split("/")[1]
        print("**************************************************************************")
        print("Starting to process vaccine: "+vaccine_name+" has "+str(len(vaccine))+" chunks")
        with FunctionExecutor() as fexec:
            for chunk in vaccine:
                fexec.call_async(process_chunk, chunk)
            
            fexec.wait()
        print("**************************************************************************")
    
    print("**************************************************************************")
    print("Starting to reduce chunks")
    with FunctionExecutor() as fexec:
        for vaccine in vaccine_path_list:
            vaccine_name = vaccine.split("/")[1]
            print("Starting to reduce "+vaccine_name+" chunks")
            fexec.map_reduce(read_csv, "cos://practica-2-sd-twitter-vacunas/ProcesedChunks/"+vaccine_name+"/", reduce_csv)
        fexec.wait()
    print("Tweets preprocessed successfully!")
    print("**************************************************************************")