#!/usr/bin/env python3
from lithops.storage.cloud_proxy import open, os
import pandas as pd
import matplotlib.pyplot as plt
from functools import reduce
import numpy as np

def ls(path):
    paths = []
    for root, _, files in os.walk(path):
        for file in files:
            paths.append(root+"/"+file)
    return paths

def process_vaccine(vaccine_path):
    my_headers = [ "vaccine","id","date","loc","geo","sentiment"]
    with open(vaccine_path, mode="r") as f:
        df = pd.read_csv(f, names=my_headers, header=None)
    plot_df = pd.DataFrame()
    plot_df[df['vaccine'][0]] = df['sentiment'].head(17000)
    return plot_df

def join_df(df1, df2):
    return pd.concat([df1, df2], axis=1)

def append_df(df1, df2):
    return pd.concat([df1.rename(columns={df1.columns[0]:'Vaccination'}), df2.rename(columns={df2.columns[0]:'Vaccination'})])

def bin_vaccine(df):
    pos = df[df[df.columns[0]] >= 0.05].count()[0]
    neg = df[df[df.columns[0]] <= -0.05].count()[0]
    neut = df.count()[0]-(pos + neg)
    bin_df = pd.DataFrame(columns=['Vaccine', 'Positive', 'Neutral', 'Negative'])
    return bin_df.append({'Vaccine':df.columns[0], 'Positive':pos, 'Neutral':neut, 'Negative':neg}, ignore_index=True)    


if __name__ == "__main__":
    paths = ls("Processed")
    res = list(map(process_vaccine, paths))

    #Global histogram
    appended_df = reduce(append_df, res)
    hist_ax = appended_df.plot(kind='hist', bins=41, xticks=np.arange(-1, 1.1, 0.1), figsize=(10, 5))
    hist_ax.set_xlabel('Sentiment')
    plt.tight_layout()
    plt.savefig("Plots/Global_histogram.png")

    #Histogram per vaccine
    for vaccine in res:
        hist_ax = vaccine.plot(kind='hist', bins=41, xticks=np.arange(-1, 1.1, 0.1), figsize=(10, 5))
        hist_ax.set_xlabel('Sentiment')
        plt.tight_layout()
        plt.savefig("Plots/"+vaccine.columns[0]+"_histogram.png")

    #Binarize vaccines
    bin_dataset = list(map(bin_vaccine, res))
    bin_datased_joined = reduce(append_df, bin_dataset)
    bin_dataset_ax = bin_datased_joined.plot(x='Vaccination', kind='bar', stacked=True, color={'Positive': '#28a745', 'Negative':'#dc3545', 'Neutral':'#ffc107'}, yticks=np.arange(0, 18001, 2000))
    bin_dataset_ax.set_xlabel('Vaccine')
    bin_dataset_ax.set_ylabel('People')
    plt.tight_layout()
    plt.savefig("Plots/binarized_data.png", dpi=199)
