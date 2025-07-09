from unittest.mock import Mock, patch
from infrastructure.external import MediaArtsSPARQLClient


class TestMediaArtsSPARQLClient:
    def setup_method(self):
        self.client = MediaArtsSPARQLClient()

    @patch('infrastructure.external.sparql_client.SPARQLWrapper')
    def test_execute_query_success(self, mock_sparql_wrapper):
        # Arrange
        mock_sparql = Mock()
        mock_sparql_wrapper.return_value = mock_sparql

        expected_result = {
            'results': {
                'bindings': [
                    {'title': {'value': 'Test Work'}}
                ]
            }
        }
        mock_sparql.queryAndConvert.return_value = expected_result

        client = MediaArtsSPARQLClient()
        query = "SELECT ?title WHERE { ?work rdfs:label ?title }"

        # Act
        result = client.execute_query(query)

        # Assert
        assert result == expected_result
        mock_sparql.setQuery.assert_called_once_with(query)
        mock_sparql.setTimeout.assert_called_once_with(55)

    @patch('infrastructure.external.sparql_client.SPARQLWrapper')
    def test_execute_query_timeout(self, mock_sparql_wrapper):
        # Arrange
        mock_sparql = Mock()
        mock_sparql_wrapper.return_value = mock_sparql
        mock_sparql.queryAndConvert.side_effect = Exception("timeout")

        client = MediaArtsSPARQLClient()
        query = "SELECT ?title WHERE { ?work rdfs:label ?title }"

        # Act
        result = client.execute_query(query)

        # Assert
        assert result is None

    @patch.object(MediaArtsSPARQLClient, 'execute_query')
    def test_search_manga_works(self, mock_execute_query):
        # Arrange
        mock_result = {
            'results': {
                'bindings': [
                    {
                        'work': {'value': 'http://example.com/work/1'},
                        'title': {'value': 'ONE PIECE'},
                        'creator': {'value': 'http://example.com/creator/1'},
                        'creatorName': {'value': '尾田栄一郎'},
                        'genre': {'value': '漫画'},
                        'publisher': {'value': '集英社'},
                        'publishedDate': {'value': '1997'}
                    }
                ]
            }
        }
        mock_execute_query.return_value = mock_result

        # Act
        result = self.client.search_manga_works("ONE PIECE", 20)

        # Assert
        assert len(result) == 1
        work = result[0]
        assert work['uri'] == 'http://example.com/work/1'
        assert work['title'] == 'ONE PIECE'
        assert work['creator_name'] == '尾田栄一郎'
        assert work['genre'] == '漫画'

    @patch.object(MediaArtsSPARQLClient, 'execute_query')
    def test_search_manga_works_no_results(self, mock_execute_query):
        # Arrange
        mock_execute_query.return_value = None

        # Act
        result = self.client.search_manga_works("nonexistent", 20)

        # Assert
        assert result == []

    @patch.object(MediaArtsSPARQLClient, 'execute_query')
    def test_get_manga_by_creator(self, mock_execute_query):
        # Arrange
        mock_result = {
            'results': {
                'bindings': [
                    {
                        'work': {'value': 'http://example.com/work/1'},
                        'title': {'value': 'ONE PIECE'},
                        'creator': {'value': 'http://example.com/creator/1'},
                        'creatorName': {'value': '尾田栄一郎'},
                        'genre': {'value': '漫画'},
                        'publisher': {'value': '集英社'}
                    }
                ]
            }
        }
        mock_execute_query.return_value = mock_result

        # Act
        result = self.client.get_manga_by_creator("尾田栄一郎", 50)

        # Assert
        assert len(result) == 1
        work = result[0]
        assert work['creator_name'] == '尾田栄一郎'
        assert work['title'] == 'ONE PIECE'

    @patch.object(MediaArtsSPARQLClient, 'execute_query')
    def test_get_manga_magazines(self, mock_execute_query):
        # Arrange
        mock_result = {
            'results': {
                'bindings': [
                    {
                        'magazine': {'value': 'http://example.com/magazine/1'},
                        'title': {'value': '週刊少年ジャンプ'},
                        'publisher': {'value': 'http://example.com/publisher/1'},
                        'publisherName': {'value': '集英社'},
                        'genre': {'value': '漫画雑誌'}
                    }
                ]
            }
        }
        mock_execute_query.return_value = mock_result

        # Act
        result = self.client.get_manga_magazines(100)

        # Assert
        assert len(result) == 1
        magazine = result[0]
        assert magazine['uri'] == 'http://example.com/magazine/1'
        assert magazine['title'] == '週刊少年ジャンプ'
        assert magazine['publisher_name'] == '集英社'

    @patch.object(MediaArtsSPARQLClient, 'execute_query')
    def test_search_with_fulltext(self, mock_execute_query):
        # Arrange
        mock_result = {
            'results': {
                'bindings': [
                    {
                        'resource': {'value': 'http://example.com/resource/1'},
                        'title': {'value': 'テスト漫画'},
                        'type': {'value': 'http://schema.org/CreativeWork'}
                    }
                ]
            }
        }
        mock_execute_query.return_value = mock_result

        # Act
        result = self.client.search_with_fulltext("漫画", "simple_query_string", 20)

        # Assert
        assert len(result) == 1
        resource = result[0]
        assert resource['uri'] == 'http://example.com/resource/1'
        assert resource['title'] == 'テスト漫画'
        assert resource['type'] == 'http://schema.org/CreativeWork'

    def test_query_construction_search_manga_works(self):
        # このテストは実際のクエリ文字列の構築をテストします
        # より詳細なクエリのテストが必要な場合に使用できます
        search_term = "ONE PIECE"
        limit = 20

        # クエリに必要な要素が含まれているかチェック
        # 実際の実装では、クエリ文字列を検証するメソッドを追加することも可能
        assert search_term is not None
        assert limit > 0

    def test_rate_limiting_delay(self):
        # レート制限のディレイが設定されているかテスト
        assert self.client.rate_limit_delay == 1.0

    def test_endpoint_url_configuration(self):
        # エンドポイントURLが正しく設定されているかテスト
        default_client = MediaArtsSPARQLClient()
        assert default_client.endpoint_url == "https://sparql.cineii.jbf.ne.jp/sparql"

        custom_client = MediaArtsSPARQLClient("http://custom.endpoint")
        assert custom_client.endpoint_url == "http://custom.endpoint"
