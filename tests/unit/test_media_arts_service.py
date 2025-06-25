import pytest
from unittest.mock import Mock, patch
from domain.services import MediaArtsDataService
from infrastructure.external import MediaArtsSPARQLClient


class TestMediaArtsDataService:
    def setup_method(self):
        self.mock_sparql_client = Mock(spec=MediaArtsSPARQLClient)
        self.service = MediaArtsDataService(self.mock_sparql_client)

    def test_search_manga_data_success(self):
        # Arrange
        search_term = "ONE PIECE"
        mock_works_data = [
            {
                'uri': 'http://example.com/work/1',
                'title': 'ONE PIECE',
                'creator_uri': 'http://example.com/creator/1',
                'creator_name': '尾田栄一郎',
                'genre': '漫画',
                'publisher': '集英社',
                'published_date': '1997'
            }
        ]
        self.mock_sparql_client.search_manga_works.return_value = mock_works_data

        # Act
        result = self.service.search_manga_data(search_term)

        # Assert
        assert 'nodes' in result
        assert 'edges' in result
        assert len(result['nodes']) == 2  # work + creator
        assert len(result['edges']) == 1  # created relationship
        
        # Check work node
        work_node = next(node for node in result['nodes'] if node['type'] == 'work')
        assert work_node['label'] == 'ONE PIECE'
        assert work_node['properties']['source'] == 'media_arts_db'
        
        # Check creator node
        creator_node = next(node for node in result['nodes'] if node['type'] == 'author')
        assert creator_node['label'] == '尾田栄一郎'
        
        # Check edge
        edge = result['edges'][0]
        assert edge['type'] == 'created'
        assert edge['source'] == 'http://example.com/creator/1'
        assert edge['target'] == 'http://example.com/work/1'

    def test_search_manga_data_empty_result(self):
        # Arrange
        search_term = "nonexistent"
        self.mock_sparql_client.search_manga_works.return_value = []

        # Act
        result = self.service.search_manga_data(search_term)

        # Assert
        assert result == {'nodes': [], 'edges': []}

    def test_search_manga_data_exception_handling(self):
        # Arrange
        search_term = "test"
        self.mock_sparql_client.search_manga_works.side_effect = Exception("SPARQL error")

        # Act
        result = self.service.search_manga_data(search_term)

        # Assert
        assert result == {'nodes': [], 'edges': []}

    def test_get_creator_works_success(self):
        # Arrange
        creator_name = "尾田栄一郎"
        mock_works_data = [
            {
                'uri': 'http://example.com/work/1',
                'title': 'ONE PIECE',
                'creator_uri': 'http://example.com/creator/1',
                'creator_name': '尾田栄一郎',
                'genre': '漫画',
                'publisher': '集英社',
                'published_date': '1997'
            },
            {
                'uri': 'http://example.com/work/2',
                'title': 'ROMANCE DAWN',
                'creator_uri': 'http://example.com/creator/1',
                'creator_name': '尾田栄一郎',
                'genre': '漫画',
                'publisher': '集英社',
                'published_date': '1996'
            }
        ]
        self.mock_sparql_client.get_manga_by_creator.return_value = mock_works_data

        # Act
        result = self.service.get_creator_works(creator_name)

        # Assert
        assert 'nodes' in result
        assert 'edges' in result
        assert len(result['nodes']) == 3  # 1 creator + 2 works
        assert len(result['edges']) == 2  # 2 created relationships

    def test_get_manga_magazines_graph_success(self):
        # Arrange
        mock_magazines_data = [
            {
                'uri': 'http://example.com/magazine/1',
                'title': '週刊少年ジャンプ',
                'publisher_uri': 'http://example.com/publisher/1',
                'publisher_name': '集英社',
                'genre': '漫画雑誌'
            }
        ]
        self.mock_sparql_client.get_manga_magazines.return_value = mock_magazines_data

        # Act
        result = self.service.get_manga_magazines_graph()

        # Assert
        assert 'nodes' in result
        assert 'edges' in result
        assert len(result['nodes']) == 2  # magazine + publisher
        assert len(result['edges']) == 1  # publishes relationship
        
        # Check magazine node
        magazine_node = next(node for node in result['nodes'] if node['type'] == 'magazine')
        assert magazine_node['label'] == '週刊少年ジャンプ'
        
        # Check publisher node
        publisher_node = next(node for node in result['nodes'] if node['type'] == 'publisher')
        assert publisher_node['label'] == '集英社'

    def test_search_with_fulltext_success(self):
        # Arrange
        search_term = "漫画"
        mock_results = [
            {
                'uri': 'http://example.com/resource/1',
                'title': 'テスト漫画',
                'type': 'http://schema.org/CreativeWork'
            }
        ]
        self.mock_sparql_client.search_with_fulltext.return_value = mock_results

        # Act
        result = self.service.search_with_fulltext(search_term)

        # Assert
        assert 'nodes' in result
        assert 'edges' in result
        assert len(result['nodes']) == 1
        assert len(result['edges']) == 0  # 全文検索では関係性情報が限定的
        
        node = result['nodes'][0]
        assert node['label'] == 'テスト漫画'
        assert node['properties']['source'] == 'media_arts_db'

    def test_no_duplicate_nodes(self):
        # Arrange
        search_term = "test"
        mock_works_data = [
            {
                'uri': 'http://example.com/work/1',
                'title': 'Work 1',
                'creator_uri': 'http://example.com/creator/1',
                'creator_name': 'Creator 1',
                'genre': '漫画',
                'publisher': 'Publisher 1',
                'published_date': '2000'
            },
            {
                'uri': 'http://example.com/work/1',  # Same URI
                'title': 'Work 1',
                'creator_uri': 'http://example.com/creator/1',  # Same creator URI
                'creator_name': 'Creator 1',
                'genre': '漫画',
                'publisher': 'Publisher 1',
                'published_date': '2000'
            }
        ]
        self.mock_sparql_client.search_manga_works.return_value = mock_works_data

        # Act
        result = self.service.search_manga_data(search_term)

        # Assert
        # Should have only unique nodes despite duplicate input data
        unique_node_ids = [node['id'] for node in result['nodes']]
        assert len(unique_node_ids) == len(set(unique_node_ids))
        assert len(result['nodes']) == 2  # 1 work + 1 creator, no duplicates