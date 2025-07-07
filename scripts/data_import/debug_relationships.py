import os

from neo4j import GraphDatabase

# Neo4j接続設定
neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
neo4j_user = os.getenv("NEO4J_USER", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")


def debug_naruto_relationships():
    """NARUTOに関連するリレーションシップをデバッグ"""
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as session:
        print("=== NARUTO関連のWork検索 ===")

        # NARUTO関連のWorkを検索
        result = session.run(
            """
            MATCH (w:Work)
            WHERE w.title CONTAINS 'NARUTO' OR w.title CONTAINS 'Naruto'
            RETURN w.id, w.title
            LIMIT 5
        """
        )

        works = list(result)
        print(f"Found {len(works)} NARUTO works:")
        for work in works:
            print(f"  ID: {work['w.id']}, Title: {work['w.title']}")

        if works:
            # 最初のNARUTO作品のリレーションシップを確認
            first_work_id = works[0]["w.id"]
            print(f"\n=== '{first_work_id}'のリレーションシップ ===")

            # 作品から著者への関係
            result = session.run(
                """
                MATCH (w:Work {id: $work_id})-[r:CREATED_BY]->(a:Author)
                RETURN r, a.id, a.name
                LIMIT 5
            """,
                work_id=first_work_id,
            )

            authors = list(result)
            print(f"Authors connected to this work: {len(authors)}")
            for author in authors:
                print(f"  Author ID: {author['a.id']}, Name: {author['a.name']}")

            # 作品から雑誌への関係
            result = session.run(
                """
                MATCH (w:Work {id: $work_id})-[r:PUBLISHED_IN]->(m:Magazine)
                RETURN r, m.id, m.title
                LIMIT 5
            """,
                work_id=first_work_id,
            )

            magazines = list(result)
            print(f"Magazines connected to this work: {len(magazines)}")
            for magazine in magazines:
                print(f"  Magazine ID: {magazine['m.id']}, Title: {magazine['m.title']}")

        print("\n=== 全体のリレーションシップ統計 ===")

        # 全体の統計
        result = session.run("MATCH ()-[r:CREATED_BY]->() RETURN count(r) AS count")
        created_by_count = result.single()["count"]
        print(f"Total CREATED_BY relationships: {created_by_count}")

        result = session.run("MATCH ()-[r:PUBLISHED_IN]->() RETURN count(r) AS count")
        published_in_count = result.single()["count"]
        print(f"Total PUBLISHED_IN relationships: {published_in_count}")

        result = session.run("MATCH ()-[r:PUBLISHED_BY]->() RETURN count(r) AS count")
        published_by_count = result.single()["count"]
        print(f"Total PUBLISHED_BY relationships: {published_by_count}")

        print("\n=== サンプルのCREATED_BY関係 ===")
        result = session.run(
            """
            MATCH (w:Work)-[r:CREATED_BY]->(a:Author)
            RETURN w.title, a.name, a.id
            LIMIT 5
        """
        )

        for record in result:
            print(f"Work: {record['w.title']} -> Author: {record['a.name']} (ID: {record['a.id']})")

    driver.close()


if __name__ == "__main__":
    debug_naruto_relationships()
