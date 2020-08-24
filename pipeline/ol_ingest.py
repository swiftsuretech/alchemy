"""
Author: David Whitehouse (dave@davewhitehouse.co.uk)
Date:   23 Aug 2020
Scope: Simple script to ingest the relevant excerpts from offshore leaks.
"""

import csv
from elasticsearch import helpers, Elasticsearch

entities = "../raw_data/os_curated/OL_entities.csv"
people = "../raw_data/os_curated/OL_people.csv"

es = Elasticsearch(hosts='alchemist')


def stream_ol_data(csv_file):
    """Stream company data from the large csv file to the bulk helper"""
    # Read in our logfile into the completed list
    with open(csv_file, 'r') as working_file:
        print(f"Working on file {csv_file}")
        items = csv.DictReader(working_file)
        for item in items:
            if csv_file.find('entities') > 1 and \
                    (csv_file == ['incorporation_date'] == "" or len(item['incorporation_date']) > 10):
                item['incorporation_date'] = None
            yield {
                "_index": f"ol{csv_file[csv_file.rfind('_'):-4]}",
                "_source": item
            }


if __name__ == '__main__':
    try:
        # helpers.bulk(es, stream_ol_data(entities), request_timeout=40)
        helpers.bulk(es, stream_ol_data(people), request_timeout=40)
    except Exception as e:
        print(f'There was an error writing to Elastic. Error was {e}')
