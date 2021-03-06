import os
import sys
import py2neo
import json
import requests
import csv
import logging


from cypher_queries import *

# logging
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
LOG_LEVEL = os.getenv('LOG_LEVEL', "INFO")
log.setLevel(LOG_LEVEL)

# neo4j connection
neo4j_config_str = os.getenv('NEO4J', '{"host": "localhost"}')
neo4j_config_dict = json.loads(neo4j_config_str)
graph = py2neo.Graph(**neo4j_config_dict)

ENV = os.getenv('ENV', 'prod')


def download_csv(url, filename):
    data = requests.get(url)
    open(filename, 'wb').write(data.content)


if __name__ == "__main__":

    graph.run(create_named_entity_constraint)

    # Import named entities
    log.info("Download named entities")
    PUBMED_ABSTRACT_DOWNLOAD_URL = 'https://drive.google.com/u/0/uc?id=1Rlv70gNtalFp4T4XtpI6psJdJJzZFTTY&export=download'
    PUBMED_ABSTRACT_FILE = 'pubmed_abstracts.csv'

    download_csv(PUBMED_ABSTRACT_DOWNLOAD_URL, PUBMED_ABSTRACT_FILE)

    # Import named entities for abstracts

    abstract_parameters = []
    with open(PUBMED_ABSTRACT_FILE) as csvfile:
        reader = csv.DictReader(csvfile)
        for index, row in enumerate(reader):
            abstract_parameters.append({'pubmed_id': row['pubmed_id'], 'entity_type': row['entity_type'],
                                        'entity_value': row['entity_value'], 'entity_id': row['covidgraph_id']})
            # Batch by 1000 rows
            if (index != 0) and (index % 1000 == 0):
                log.info(
                    'importing {} batch abstract entities'.format(index / 1000))
                r = graph.run(pubmed_abstract_query, {
                    'parameters': abstract_parameters})
                abstract_parameters = []
        # import the rest
        r = graph.run(pubmed_abstract_query, {
            'parameters': abstract_parameters})

    # Import named entities external ids for genes

    PUBMED_GENE_EXTERNALIDS_DOWNLOAD_URL = 'https://drive.google.com/u/0/uc?id=1KgJPBYB8D4_hN7wbiu0XOOM-lQdV8EgP&export=download'
    PUBMED_GENE_EXTERNALIDS_FILE = 'pubmed_gene_external_ids.csv'

    download_csv(PUBMED_GENE_EXTERNALIDS_DOWNLOAD_URL,
                 PUBMED_GENE_EXTERNALIDS_FILE)
    external_ids_parameters = []
    with open(PUBMED_GENE_EXTERNALIDS_FILE) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(reader):
            bern_id = row[3]
            external_ids = row[1].split("|")
            external_ids_parameters.append(
                {'entity_id': bern_id, 'external_ids': external_ids})
            # Batch by 1000 rows
            if (index != 0) and (index % 1000 == 0):
                log.info(
                    'importing {} batch gene external ids'.format(index / 1000))
                r = graph.run(import_external_ids_query, {
                    'parameters': external_ids_parameters})
                external_ids_parameters = []
        # import the rest
        r = graph.run(import_external_ids_query, {
            'parameters': external_ids_parameters})

    # Import named entities external ids for diseases

    PUBMED_DISEASE_EXTERNALIDS_DOWNLOAD_URL = "https://drive.google.com/u/0/uc?id=1guHxBbUksuDx58zKh8o0d0dgs7klotFT&export=download"
    PUBMED_DISEASE_EXTERNALIDS_FILE = "pubmed_disease_external_ids.csv"

    download_csv(PUBMED_DISEASE_EXTERNALIDS_DOWNLOAD_URL,
                 PUBMED_DISEASE_EXTERNALIDS_FILE)

    external_ids_parameters = []
    with open(PUBMED_DISEASE_EXTERNALIDS_FILE) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(reader):
            bern_id = row[0]
            external_ids = row[1].split(", ")
            external_ids_parameters.append(
                {'entity_id': bern_id, 'external_ids': external_ids})

            # Batch by 1000 rows
            if (index != 0) and (index % 1000 == 0):
                log.info('importing {} batch disease external ids'.format(
                    index / 1000))
                r = graph.run(import_external_ids_query, {
                    'parameters': external_ids_parameters})
                external_ids_parameters = []
        # import the rest
        r = graph.run(import_external_ids_query, {
            'parameters': external_ids_parameters})

    # Import named entities external ids for drugs

    PUBMED_DRUG_EXTERNALIDS_DOWNLOAD_URL = "https://drive.google.com/u/0/uc?id=1zq-za_1OMCrrJaVwIj-dwIHogqVQ9n0G&export=download"
    PUBMED_DRUG_EXTERNALIDS_FILE = "pubmed_drug_external_ids.csv"

    download_csv(PUBMED_DRUG_EXTERNALIDS_DOWNLOAD_URL,
                 PUBMED_DRUG_EXTERNALIDS_FILE)

    external_ids_parameters = []
    with open(PUBMED_DRUG_EXTERNALIDS_FILE) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for index, row in enumerate(reader):
            bern_id = row[0]
            external_ids = row[1].split(", ")
            external_ids_parameters.append(
                {'entity_id': bern_id, 'external_ids': external_ids})
            # Batch by 1000 rows
            if (index != 0) and (index % 1000 == 0):
                log.info(
                    'importing {} batch drug external ids'.format(index / 1000))
                r = graph.run(import_external_ids_query, {
                    'parameters': external_ids_parameters})
                external_ids_parameters = []
        # import the rest
        r = graph.run(import_external_ids_query, {
            'parameters': external_ids_parameters})

    # Species tranformation
    #
    # You can get a NCBI taxonomy ID by removing the last two digits of a BERN species ID.
    #
    # Example.
    # 1009505 -> 10095
    #
    # Mus sp., mice
    # https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=10095

    graph.run(species_ncbi_query)
