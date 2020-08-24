"""
Author: David Whitehouse (dave@davewhitehouse.co.uk)
Date:   23 Aug 2020
Scope: Breaks up our large BasicCompanyData csv file into chunks of 10000 lines. This
should prevent timeouts during ingestion and prevent re-ingesting the whole lot should
we fail half way through.
"""

import pandas as pd
import os
import time

root = "../raw_data/ch_"
company_master = f"{root}companies/BasicCompanyData.csv"
people_master = f"{root}people/persons.txt"
chunksize = 10000


def split_companies():
    """Companies info is a single csv. Use Pandas to split it. Files will be named by
    timestamp so they can be easily ordered."""
    for i, chunk in enumerate(pd.read_csv(company_master, chunksize=chunksize)):
        chunk.to_csv(f"{root}companies/chunks/{time.time()}.csv", index=False)


def split_people():
    """Splits the people txt file into chunks and moves them into the chunks folder. Using
    the shell command 'split' here as the file is already nicely formatted in json / line"""
    split_command = f"split -l {chunksize} {people_master}"
    os.system(split_command)


if __name__ == '__main__':
    split_people()
    split_companies()
