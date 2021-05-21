#!/usr/bin/env python3
from lithops import FunctionExecutor
from lithops.storage.cloud_proxy import open, os
from lithops.multiprocessing import Pool

def remoteList(vaccine):
    dirname = os.path.dirname("/RawData/"+vaccine+"/")
    return vaccine, len(os.listdir(dirname))

if __name__ == "__main__":
    total = 0
    vaccineList = ["Janssen", "Pfizer", "Moderna", "astrazeneca", "sputnik"]
    with Pool() as pool:
        res = pool.map(remoteList, vaccineList)
        for vaccine, elems in res:
            print(vaccine+" = "+str(elems))
            total += elems

        print("----------------------------------")
        print("Total tweets = "+str(total))
