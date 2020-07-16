import os
import sys
from neo4j import GraphDatabase
import requests
import csv

PUBMED_ABSTRACT_DOWNLOAD_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTkdcbidz1y98OhKHhAGenIPlv2LW8BcP7ezT-jAXkwWbSHKyW_ed4kmek07ZfQPH7UAw-zYnJ-2Xvh/pub?output=csv'
PUBMED_ABSTRACT_FILE = 'pubmed_abstracts.csv'

def download_dataset():
    data = requests.get(PUBMED_ABSTRACT_DOWNLOAD_URL)
    open(PUBMED_ABSTRACT_FILE, 'wb').write(data.content)

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )
    PARENT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(PARENT_DIR))

dataset_path = os.path.join(PARENT_DIR,"dataset")

download_dataset()

neo4j_url = os.getenv('GC_NEO4J_URL', 'bolt://localhost:7687')
neo4j_user = os.getenv('GC_NEO4J_USER', 'neo4j')
neo4j_pw = os.getenv('GC_NEO4J_PASSWORD', 'test')
ENV = os.getenv('ENV', 'prod')

driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_pw))

pubmed_abstract_query = """
UNWIND $parameters as data
MATCH (p:PaperID)
WHERE p.type = 'pubmed_id' AND p.id = toString(data.pubmed_id)
MATCH (p)<-[:PAPER_HAS_PAPERID]-()-[:PAPER_HAS_ABSTRACTCOLLECTION]->()-[:ABSTRACTCOLLECTION_HAS_ABSTRACT]->(abstract)
MERGE (n:NamedEntity{type:data.entity_type, value:data.entity_value})
ON CREATE SET n.id = data.entity_id
MERGE (abstract)-[:MENTIONS]->(n)
"""

create_named_entity_index = """
CREATE INDEX named_entity_index FOR (n:NamedEntity)
ON (n.type, n.value)
"""

parameters = []
with driver.session() as session:
    try:
        session.run(create_named_entity_index)
    except:
        pass
    
    with open(PUBMED_ABSTRACT_FILE) as csvfile:
        reader = csv.DictReader(csvfile)
        for index, row in enumerate(reader):
            parameters.append({'pubmed_id': row['pubmedid'], 'entity_type': row['entity_type'],'entity_value': row['entity_text'], 'entity_id':row['entity_id']})
            # Batch by 1000 rows
            if (index != 0) and (index % 1000 == 0):
                print('importing {} batch'.format(index / 1000))
                session.run(pubmed_abstract_query, {'parameters': parameters})
                parameters = []
        # import the rest
        session.run(pubmed_abstract_query, {'parameters': parameters})


