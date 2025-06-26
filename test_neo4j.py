from neo4j import GraphDatabase


def test_connection():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        # Test simple query
        result = session.run("MATCH (n) RETURN count(n) as count")
        count = result.single()["count"]
        print(f"Total nodes: {count}")

        # Test search query
        search_query = "MATCH (n) WHERE toLower(n.title) CONTAINS toLower($searchTerm) OR toLower(n.name) CONTAINS toLower($searchTerm) RETURN n LIMIT 5"
        result = session.run(search_query, searchTerm="ONE PIECE")

        for record in result:
            node = record["n"]
            print(f"Node: {node}")

    driver.close()


if __name__ == "__main__":
    test_connection()
