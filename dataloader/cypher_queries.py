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

species_ncbi_query = """

MATCH (n:NamedEntity)
WHERE n.type='species' AND NOT size(n.id) = 32
WITH n, ["NCBI:" + substring(n.id,0, size(n.id) - 2)] as external_ids
SET n.external_ids = external_ids

"""