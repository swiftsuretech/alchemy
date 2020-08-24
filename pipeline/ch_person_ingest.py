"""
Author: David Whitehouse (dave@davewhitehouse.co.uk)
Date:   23 Aug 2020
Scope: Simple script to ingest the monthly json list from Companies House containing details of all
UK persons with significant control. Parses the document and upserts the contents into the existing
companies ingest using company number as the key.
"""

from elasticsearch import helpers, Elasticsearch
import os
import json

persons_master = "../raw_data/ch_people/persons.txt"
chunk_dir = "../raw_data/ch_people/chunks/"
log = "./ch_person_ingest.log"
es = Elasticsearch('alchemist')


def stream_names():
    """Open the people text file and yield the dictionary by line as json back to the bulk indexer"""
    # Read in our logfile into the completed list
    completed = []
    with open(log, 'r') as completed_items:
        for completed_item in completed_items:
            completed.append(completed_item[0:3])
    for chunk in sorted(os.listdir(chunk_dir)):
        if chunk not in completed:
            with open(f"{chunk_dir}{chunk}", 'r') as persons:
                print(f"Working on file {chunk}")
                for person in persons:
                    company_number = json.loads(person)['company_number']
                    yield {
                        "_index": "ch_person",
                        "_source": person
                    }
                completed.append(persons)
                with open(log, 'a') as logger:
                    logger.write(f"{chunk}\n")
        else:
            print(f"Already ingested file {chunk}. Moving on")


if __name__ == '__main__':
    try:
        helpers.bulk(es, stream_names(), request_timeout=40)
    except Exception as e:
        print(f'There was an error writing to Elastic. Error was {e}')
