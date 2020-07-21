import os
import sys
from neo4j import GraphDatabase
import requests
import csv

neo4j_url = os.getenv('GC_NEO4J_URL', 'bolt://db-dev.covidgraph.org:7687')
neo4j_user = os.getenv('GC_NEO4J_USER', 'neo4j')
neo4j_pw = os.getenv('GC_NEO4J_PASSWORD', 'CureCovid46')
ENV = os.getenv('ENV', 'prod')

PUBMED_ABSTRACT_DOWNLOAD_URL = 'https://drive.google.com/u/0/uc?id=1Rlv70gNtalFp4T4XtpI6psJdJJzZFTTY&export=download'
PUBMED_ABSTRACT_FILE = 'pubmed_abstracts.csv'

PUBMED_EXTERNALIDS_DOWNLOAD_URL = 'https://drive.google.com/u/0/uc?id=1KgJPBYB8D4_hN7wbiu0XOOM-lQdV8EgP&export=download'
PUBMED_EXTERNALIDS_FILE = 'pubmed_external_ids.csv'

def download_csv(url, filename):
    data = requests.get(url)
    open(filename, 'wb').write(data.content)

pubmed_abstract_query = """
    UNWIND $parameters as data
    MATCH (p:PaperID)
    WHERE p.type = 'pubmed_id' AND p.id = toString(data.pubmed_id)
    MATCH (p)<-[:PAPER_HAS_PAPERID]-()-[:PAPER_HAS_ABSTRACTCOLLECTION]->()-[a:ABSTRACTCOLLECTION_HAS_ABSTRACT]->(abstract)
    WHERE a.position = 0
    MERGE (n:NamedEntity{id:data.entity_id})
    ON CREATE SET n += {type:data.entity_type, value:data.entity_value}
    MERGE (abstract)-[m:MENTIONS]->(n)
    ON CREATE SET m.count = 1
    ON MATCH SET m.count = m.count + 1
    """

create_named_entity_constraint = """
CREATE CONSTRAINT ON (n:NamedEntity) 
ASSERT n.id IS UNIQUE;
"""

import_external_ids_query = """
UNWIND $parameters as row
MATCH (n:NamedEntity)
WHERE n.id = row.entity_id
SET n.external_ids = row.external_ids

"""

if __name__ == "__main__":
    """
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )
    PARENT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(PARENT_DIR))

    dataset_path = os.path.join(PARENT_DIR,"dataset")
    """
    # Connect to Neo4j

    driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_pw))
    
    # Import named entities

    download_csv(PUBMED_ABSTRACT_DOWNLOAD_URL, PUBMED_ABSTRACT_FILE)

    abstract_parameters = []
    external_ids_parameters = []
    with driver.session() as session:
        
        try:
            session.run(create_named_entity_constraint)
        except:
            pass
        
        with open(PUBMED_ABSTRACT_FILE) as csvfile:
            reader = csv.DictReader(csvfile)
            for index, row in enumerate(reader):
                abstract_parameters.append({'pubmed_id': row['pubmed_id'], 'entity_type': row['entity_type'],'entity_value': row['entity_value'], 'entity_id':row['covidgraph_id']})
                # Batch by 1000 rows
                if (index != 0) and (index % 1000 == 0):
                    print('importing {} batch abstract entities'.format(index / 1000))
                    r = session.run(pubmed_abstract_query, {'parameters': abstract_parameters})
                    abstract_parameters = []
            # import the rest
            r = session.run(pubmed_abstract_query, {'parameters': abstract_parameters})
        
        # Import named entities external ids
        
        download_csv(PUBMED_EXTERNALIDS_DOWNLOAD_URL, PUBMED_EXTERNALIDS_FILE)

        with open(PUBMED_EXTERNALIDS_FILE) as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            for index, row in enumerate(reader):
                bern_id = row[3]
                external_ids = row[1].split("|")
                external_ids_parameters.append({'entity_id': bern_id, 'external_ids': external_ids})
                # Batch by 1000 rows
                if (index != 0) and (index % 1000 == 0):
                    print('importing {} batch external ids'.format(index / 1000))
                    r = session.run(import_external_ids_query, {'parameters': external_ids_parameters})
                    external_ids_parameters = []
            # import the rest
            r = session.run(import_external_ids_query, {'parameters': external_ids_parameters})



