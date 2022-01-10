from py2neo import Graph

NEO4J_URI = 'bolt://host.docker.internal:7687'  # https://stackoverflow.com/questions/54247144/how-do-i-tell-my-dockerised-spring-boot-neo4j-application-to-talk-to-my-neo4j-se
NEO4J_USER = 'neo4j'  # credentials are configured in docker-compose.yaml
NEO4J_PASSWORD = 'changeme'


def neo4j_import(transformed_file_path: str):
    graph = Graph(NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    # https://neo4j.com/labs/apoc/4.1/import/load-json/#load-json-examples-local
    create_child_relationships = "CALL apoc.load.json(\"file:///" + transformed_file_path + "\") " + \
        """
        YIELD value as meme
        MERGE (parent_meme:Meme {title: meme.title, url: meme.url})
        WITH parent_meme, meme
        UNWIND meme.children AS child_url
        MATCH (child_meme:Meme {url: child_url})
        MERGE (child_meme)-[:CHILD]->(parent_meme);
        """

    graph.run(create_child_relationships)
