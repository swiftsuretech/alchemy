"""
Author: David Whitehouse (dave@davewhitehouse.co.uk)
Date:   23 Aug 2020
Scope: Simple script to ingest the monthly csv from Companies House containing details of all
UK companies. Parses the document and ingests the contents into an Elastic stack running on a local
docker socket.
"""

import os
import csv
from elasticsearch import helpers, Elasticsearch

root = "../raw_data/ch_companies/chunks/"
log = "./ch_companies_ingest.log"
es = Elasticsearch(hosts='alchemist')


def stream_companies():
    """Stream company data from the large csv file to the bulk helper"""
    # Read in our logfile into the completed list
    completed = []
    with open(log, 'r') as completed_items:
        for completed_item in completed_items:
            completed.append(completed_item[:-1])
    for chunk in sorted(os.listdir(root)):
        if chunk not in completed:
            with open(f"{root}{chunk}", 'r') as working_file:
                print(f"Working on file {chunk}")
                companies = csv.DictReader(working_file)
                for company in companies:
                    yield {
                        "_index": "ch_companies",
                        "_source": company,
                        "_id": company['CompanyNumber']
                    }
            completed.append(working_file)
            with open(log, 'a') as logger:
                logger.write(f"{chunk}\n")
        else:
            print(f"Already ingested file {chunk}. Moving on")


if __name__ == '__main__':
    try:
        helpers.bulk(es, stream_companies(), request_timeout=40)
    except Exception as e:
        print(f'There was an error writing to Elastic. Error was {e}')
