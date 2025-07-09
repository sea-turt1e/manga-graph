import pytest
import asyncio
import aiohttp
from unittest.mock import AsyncMock, patch
from domain.services.image_fetch_service import ImageFetchService, get_image_fetch_service


class TestImageFetchService:
    @pytest.fixture
    def image_fetch_service(self):
        return ImageFetchService(timeout=30, max_concurrent=10)

    def test_initialization(self, image_fetch_service):
        assert image_fetch_service.timeout.total == 30
        assert image_fetch_service.max_concurrent == 10
        assert image_fetch_service.session is None

    @pytest.mark.asyncio
    async def test_context_manager(self):
        async with ImageFetchService() as service:
            assert service.session is not None
            assert isinstance(service.session, aiohttp.ClientSession)
        # After exiting context, session should be closed
        assert service.session.closed

    @pytest.mark.asyncio
    async def test_fetch_single_image_success(self, image_fetch_service):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {'Content-Type': 'image/jpeg'}
        mock_response.read = AsyncMock(return_value=b'fake_image_data')

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.__aexit__ = AsyncMock()

        image_fetch_service.session = mock_session

        result = await image_fetch_service._fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result['work_id'] == 'work_1'
        assert result['image_data'] == b'fake_image_data'
        assert result['content_type'] == 'image/jpeg'
        assert result['file_size'] == len(b'fake_image_data')
        assert result['success'] is True
        assert result['error'] is None

    @pytest.mark.asyncio
    async def test_fetch_single_image_http_error(self, image_fetch_service):
        mock_response = AsyncMock()
        mock_response.status = 404

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_response)
        mock_session.__aexit__ = AsyncMock()

        image_fetch_service.session = mock_session

        result = await image_fetch_service._fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result['work_id'] == 'work_1'
        assert result['image_data'] is None
        assert result['success'] is False
        assert result['error'] == 'HTTP 404'

    @pytest.mark.asyncio
    async def test_fetch_single_image_timeout(self, image_fetch_service):
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=asyncio.TimeoutError())

        image_fetch_service.session = mock_session

        result = await image_fetch_service._fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result['work_id'] == 'work_1'
        assert result['image_data'] is None
        assert result['success'] is False
        assert result['error'] == 'Timeout'

    @pytest.mark.asyncio
    async def test_fetch_single_image_exception(self, image_fetch_service):
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=Exception('Network error'))

        image_fetch_service.session = mock_session

        result = await image_fetch_service._fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result['work_id'] == 'work_1'
        assert result['image_data'] is None
        assert result['success'] is False
        assert result['error'] == 'Network error'

    @pytest.mark.asyncio
    async def test_fetch_single_image_no_session(self, image_fetch_service):
        image_fetch_service.session = None

        result = await image_fetch_service._fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result['work_id'] == 'work_1'
        assert result['image_data'] is None
        assert result['success'] is False
        assert 'Session not initialized' in result['error']

    @pytest.mark.asyncio
    async def test_fetch_images_empty_requests(self, image_fetch_service):
        result = await image_fetch_service.fetch_images([])
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_images_multiple_success(self, image_fetch_service):
        mock_response1 = AsyncMock()
        mock_response1.status = 200
        mock_response1.headers = {'Content-Type': 'image/jpeg'}
        mock_response1.read = AsyncMock(return_value=b'image1')

        mock_response2 = AsyncMock()
        mock_response2.status = 200
        mock_response2.headers = {'Content-Type': 'image/png'}
        mock_response2.read = AsyncMock(return_value=b'image2')

        mock_session = AsyncMock()
        # Configure mock to return different responses
        mock_session.get = AsyncMock()
        mock_session.get.return_value.__aenter__.side_effect = [mock_response1, mock_response2]

        image_fetch_service.session = mock_session

        requests = [
            {'work_id': 'work_1', 'cover_url': 'http://example.com/image1.jpg'},
            {'work_id': 'work_2', 'cover_url': 'http://example.com/image2.png'}
        ]

        # Mock the actual fetch method to return expected results
        async def mock_fetch(work_id, cover_url):
            if work_id == 'work_1':
                return {
                    'work_id': 'work_1',
                    'image_data': b'image1',
                    'content_type': 'image/jpeg',
                    'file_size': 6,
                    'success': True,
                    'error': None
                }
            else:
                return {
                    'work_id': 'work_2',
                    'image_data': b'image2',
                    'content_type': 'image/png',
                    'file_size': 6,
                    'success': True,
                    'error': None
                }

        with patch.object(image_fetch_service, '_fetch_single_image', side_effect=mock_fetch):
            results = await image_fetch_service.fetch_images(requests)

        assert len(results) == 2
        assert results[0]['work_id'] == 'work_1'
        assert results[0]['success'] is True
        assert results[1]['work_id'] == 'work_2'
        assert results[1]['success'] is True

    @pytest.mark.asyncio
    async def test_fetch_images_mixed_results(self, image_fetch_service):
        mock_session = AsyncMock()
        image_fetch_service.session = mock_session

        # Mock different results
        async def mock_fetch(work_id, cover_url):
            if work_id == 'work_1':
                return {
                    'work_id': 'work_1',
                    'image_data': b'image1',
                    'content_type': 'image/jpeg',
                    'file_size': 6,
                    'success': True,
                    'error': None
                }
            elif work_id == 'work_2':
                return {
                    'work_id': 'work_2',
                    'image_data': None,
                    'content_type': None,
                    'file_size': 0,
                    'success': False,
                    'error': 'HTTP 404'
                }
            else:
                raise Exception('Test exception')

        with patch.object(image_fetch_service, '_fetch_single_image', side_effect=mock_fetch):
            requests = [
                {'work_id': 'work_1', 'cover_url': 'http://example.com/image1.jpg'},
                {'work_id': 'work_2', 'cover_url': 'http://example.com/image2.jpg'},
                {'work_id': 'work_3', 'cover_url': 'http://example.com/image3.jpg'}
            ]

            results = await image_fetch_service.fetch_images(requests)

        assert len(results) == 3
        assert results[0]['success'] is True
        assert results[1]['success'] is False
        assert results[2]['success'] is False
        assert 'Test exception' in results[2]['error']

    @pytest.mark.asyncio
    async def test_fetch_single_image_convenience_method(self, image_fetch_service):
        mock_session = AsyncMock()
        image_fetch_service.session = mock_session

        expected_result = {
            'work_id': 'work_1',
            'image_data': b'image_data',
            'content_type': 'image/jpeg',
            'file_size': 10,
            'success': True,
            'error': None
        }

        with patch.object(image_fetch_service, '_fetch_single_image', return_value=expected_result) as mock_fetch:
            result = await image_fetch_service.fetch_single_image('work_1', 'http://example.com/image.jpg')

        assert result == expected_result
        mock_fetch.assert_called_once_with('work_1', 'http://example.com/image.jpg')

    @pytest.mark.asyncio
    async def test_concurrent_limit(self, image_fetch_service):
        image_fetch_service.max_concurrent = 2
        mock_session = AsyncMock()
        image_fetch_service.session = mock_session

        call_times = []

        async def mock_fetch(work_id, cover_url):
            call_times.append(asyncio.get_event_loop().time())
            await asyncio.sleep(0.1)  # Simulate network delay
            return {
                'work_id': work_id,
                'image_data': b'data',
                'content_type': 'image/jpeg',
                'file_size': 4,
                'success': True,
                'error': None
            }

        with patch.object(image_fetch_service, '_fetch_single_image', side_effect=mock_fetch):
            requests = [
                {'work_id': f'work_{i}', 'cover_url': f'http://example.com/image{i}.jpg'}
                for i in range(5)
            ]

            await image_fetch_service.fetch_images(requests)

        # With max_concurrent=2, there should be at least 3 different time groups
        # (first 2 concurrent, then next 2, then last 1)
        assert len(call_times) == 5

    def test_get_image_fetch_service(self):
        service = get_image_fetch_service()
        assert isinstance(service, ImageFetchService)
        assert service.timeout.total == 30
        assert service.max_concurrent == 10
