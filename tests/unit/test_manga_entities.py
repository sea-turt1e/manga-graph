from datetime import datetime
from domain.entities.manga import Author, Magazine, Work, GraphNode, GraphEdge


class TestAuthor:
    def test_author_creation_with_required_fields(self):
        author = Author(id="author_1", name="尾田栄一郎")
        assert author.id == "author_1"
        assert author.name == "尾田栄一郎"
        assert author.birth_date is None
        assert author.biography is None

    def test_author_creation_with_all_fields(self):
        birth_date = datetime(1975, 1, 1)
        author = Author(
            id="author_1",
            name="尾田栄一郎",
            birth_date=birth_date,
            biography="漫画家"
        )
        assert author.id == "author_1"
        assert author.name == "尾田栄一郎"
        assert author.birth_date == birth_date
        assert author.biography == "漫画家"


class TestMagazine:
    def test_magazine_creation_with_required_fields(self):
        magazine = Magazine(
            id="magazine_1",
            name="週刊少年ジャンプ",
            publisher="集英社"
        )
        assert magazine.id == "magazine_1"
        assert magazine.name == "週刊少年ジャンプ"
        assert magazine.publisher == "集英社"
        assert magazine.established_date is None

    def test_magazine_creation_with_all_fields(self):
        established_date = datetime(1968, 7, 11)
        magazine = Magazine(
            id="magazine_1",
            name="週刊少年ジャンプ",
            publisher="集英社",
            established_date=established_date
        )
        assert magazine.id == "magazine_1"
        assert magazine.name == "週刊少年ジャンプ"
        assert magazine.publisher == "集英社"
        assert magazine.established_date == established_date


class TestWork:
    def test_work_creation_with_required_fields(self):
        author = Author(id="author_1", name="尾田栄一郎")
        magazine = Magazine(
            id="magazine_1",
            name="週刊少年ジャンプ",
            publisher="集英社"
        )
        work = Work(
            id="work_1",
            title="ONE PIECE",
            authors=[author],
            magazines=[magazine]
        )
        assert work.id == "work_1"
        assert work.title == "ONE PIECE"
        assert len(work.authors) == 1
        assert work.authors[0] == author
        assert len(work.magazines) == 1
        assert work.magazines[0] == magazine
        assert work.publication_date is None
        assert work.genre is None
        assert work.description is None
        assert work.isbn is None
        assert work.cover_image_url is None

    def test_work_creation_with_all_fields(self):
        author = Author(id="author_1", name="尾田栄一郎")
        magazine = Magazine(
            id="magazine_1",
            name="週刊少年ジャンプ",
            publisher="集英社"
        )
        publication_date = datetime(1997, 7, 22)
        work = Work(
            id="work_1",
            title="ONE PIECE",
            authors=[author],
            magazines=[magazine],
            publication_date=publication_date,
            genre="少年漫画",
            description="海賊王を目指す物語",
            isbn="978-4-08-872509-3",
            cover_image_url="https://example.com/cover.jpg"
        )
        assert work.id == "work_1"
        assert work.title == "ONE PIECE"
        assert work.publication_date == publication_date
        assert work.genre == "少年漫画"
        assert work.description == "海賊王を目指す物語"
        assert work.isbn == "978-4-08-872509-3"
        assert work.cover_image_url == "https://example.com/cover.jpg"

    def test_work_with_multiple_authors_and_magazines(self):
        author1 = Author(id="author_1", name="作者1")
        author2 = Author(id="author_2", name="作者2")
        magazine1 = Magazine(id="mag_1", name="雑誌1", publisher="出版社1")
        magazine2 = Magazine(id="mag_2", name="雑誌2", publisher="出版社2")

        work = Work(
            id="work_1",
            title="共作作品",
            authors=[author1, author2],
            magazines=[magazine1, magazine2]
        )
        assert len(work.authors) == 2
        assert len(work.magazines) == 2
        assert author1 in work.authors
        assert author2 in work.authors
        assert magazine1 in work.magazines
        assert magazine2 in work.magazines


class TestGraphNode:
    def test_graph_node_creation(self):
        properties = {"genre": "漫画", "publisher": "集英社"}
        node = GraphNode(
            id="node_1",
            label="ONE PIECE",
            type="work",
            properties=properties
        )
        assert node.id == "node_1"
        assert node.label == "ONE PIECE"
        assert node.type == "work"
        assert node.properties == properties
        assert node.properties["genre"] == "漫画"
        assert node.properties["publisher"] == "集英社"

    def test_graph_node_with_empty_properties(self):
        node = GraphNode(
            id="node_1",
            label="Test Node",
            type="test",
            properties={}
        )
        assert node.properties == {}


class TestGraphEdge:
    def test_graph_edge_creation_without_properties(self):
        edge = GraphEdge(
            id="edge_1",
            source="node_1",
            target="node_2",
            type="created"
        )
        assert edge.id == "edge_1"
        assert edge.source == "node_1"
        assert edge.target == "node_2"
        assert edge.type == "created"
        assert edge.properties is None

    def test_graph_edge_creation_with_properties(self):
        properties = {"since": "1997", "role": "main"}
        edge = GraphEdge(
            id="edge_1",
            source="author_1",
            target="work_1",
            type="created",
            properties=properties
        )
        assert edge.id == "edge_1"
        assert edge.source == "author_1"
        assert edge.target == "work_1"
        assert edge.type == "created"
        assert edge.properties == properties
        assert edge.properties["since"] == "1997"
        assert edge.properties["role"] == "main"
