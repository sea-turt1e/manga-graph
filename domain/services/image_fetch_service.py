import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
import logging
from io import BytesIO

logger = logging.getLogger(__name__)


class ImageFetchService:
    """Service for fetching images from URLs asynchronously"""

    def __init__(self, timeout: int = 30, max_concurrent: int = 10):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_concurrent = max_concurrent
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _fetch_single_image(self, work_id: str, cover_url: str) -> Dict[str, Any]:
        """Fetch a single image from URL
        
        Args:
            work_id: Work identifier
            cover_url: URL to fetch image from
            
        Returns:
            Dict containing result data
        """
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use async context manager.")

            async with self.session.get(cover_url) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', 'image/jpeg')
                    image_data = await response.read()
                    
                    return {
                        'work_id': work_id,
                        'image_data': image_data,
                        'content_type': content_type,
                        'file_size': len(image_data),
                        'success': True,
                        'error': None
                    }
                else:
                    logger.warning(f"Failed to fetch image for work_id {work_id}: HTTP {response.status}")
                    return {
                        'work_id': work_id,
                        'image_data': None,
                        'content_type': None,
                        'file_size': 0,
                        'success': False,
                        'error': f"HTTP {response.status}"
                    }

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching image for work_id {work_id}")
            return {
                'work_id': work_id,
                'image_data': None,
                'content_type': None,
                'file_size': 0,
                'success': False,
                'error': "Timeout"
            }
        except Exception as e:
            logger.error(f"Error fetching image for work_id {work_id}: {str(e)}")
            return {
                'work_id': work_id,
                'image_data': None,
                'content_type': None,
                'file_size': 0,
                'success': False,
                'error': str(e)
            }

    async def fetch_images(self, requests: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Fetch multiple images concurrently
        
        Args:
            requests: List of dicts with 'work_id' and 'cover_url' keys
            
        Returns:
            List of result dicts
        """
        if not requests:
            return []

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_with_semaphore(work_id: str, cover_url: str):
            async with semaphore:
                return await self._fetch_single_image(work_id, cover_url)

        # Create tasks for all requests
        tasks = [
            fetch_with_semaphore(req['work_id'], req['cover_url'])
            for req in requests
        ]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception in task {i}: {str(result)}")
                processed_results.append({
                    'work_id': requests[i]['work_id'],
                    'image_data': None,
                    'content_type': None,
                    'file_size': 0,
                    'success': False,
                    'error': str(result)
                })
            else:
                processed_results.append(result)

        return processed_results

    async def fetch_single_image(self, work_id: str, cover_url: str) -> Dict[str, Any]:
        """Fetch a single image (convenience method)
        
        Args:
            work_id: Work identifier
            cover_url: URL to fetch image from
            
        Returns:
            Result dict
        """
        return await self._fetch_single_image(work_id, cover_url)


def get_image_fetch_service() -> ImageFetchService:
    """Factory function to get ImageFetchService instance"""
    return ImageFetchService()