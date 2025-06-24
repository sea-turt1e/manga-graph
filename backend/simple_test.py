from neo4j import GraphDatabase
import json

def simple_test():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    
    def get_nodes(tx):
        result = tx.run("MATCH (n) WHERE toLower(n.title) CONTAINS 'piece' OR toLower(n.name) CONTAINS 'piece' RETURN n LIMIT 5")
        return [record["n"] for record in result]
    
    with driver.session() as session:
        nodes = session.execute_read(get_nodes)
        
        node_data = []
        for node in nodes:
            node_data.append({
                "id": str(node.element_id),
                "label": node.get("title", node.get("name", "Unknown")),
                "type": list(node.labels)[0].lower() if node.labels else "unknown",
                "properties": dict(node)
            })
        
        result = {
            "nodes": node_data,
            "edges": []
        }
        
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    driver.close()

if __name__ == "__main__":
    simple_test()