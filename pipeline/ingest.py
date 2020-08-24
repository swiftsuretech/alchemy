"""
Author: David Whitehouse (dave@davewhitehouse.co.uk)
Date:   22 Aug 2020
Scope: Simple script to ingest offshore leaks and Companies house open source datasets.
Parses the document and ingests the contents into an Elastic stack running on a local
docker socket.
"""

import csv
import os
import json
from csv import reader, DictReader, DictWriter, reader
from elasticsearch import helpers, Elasticsearch

# Set up some of our global params
es = Elasticsearch('alchemist')
data_dir = f"../raw_data/"

if __name__ == '__main__':
    with open(f"{data_dir}ch_people/persons.txt", 'r') as people_master:
        with open(f"{data_dir}ch_companies/BasicCompanyData.csv", 'r') as companies_master:
            companies = DictReader(companies_master)
            for company in companies:
                company_id = company['_id']
                people_to_add = []
                for person in people_master:
                    person = json.loads(person)
                    if person['data'] == company_id:
                        people_to_add.append(person)
                        print(person)
                pass
