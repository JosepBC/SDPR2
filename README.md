# Big Data challenge
This repository has all the necessary scripts to lauch our solution for the big data challenge organized by cloud button.
## Requirements
The pakages needed by this application are located [here](requirements.txt).\
As this aplication uses [Lithops API](https://github.com/lithops-cloud/lithops) you must have a .lithops_config configured with the COS and CF backend you want to use.
## Data crawler
For this stage we have decided to crawl the data from twitter and implemented all the logic in
[twitterCrawler.py](blob/master/twitterCrawler.py).\
To launch this script use
```bash
python3 twitterCrawler.py [vaccine] [query] [max_id]
```
## Data preprocessing
The second stage is implemented in [preprocessTweets.py](blob/master/preprocessTweets.py).\
To launch the script use
```bash
python3 preprocessTweets.py
```
## Plots
The final stage is implemented in [graph.py](blob/master/graph.py), we have decided to make de following plots:
1. Global histogram
2. Histogram per vaccine
3. Sentiment stacked bar

To launch this stage use
```bash
python3 graph.py
```