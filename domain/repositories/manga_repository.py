from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from domain.entities import Work, Author, Magazine


class MangaRepository(ABC):
    @abstractmethod
    def search_graph(self, search_term: str, depth: int = 2) -> Dict[str, List]:
        pass

    @abstractmethod
    def get_all_authors(self) -> List[Author]:
        pass

    @abstractmethod
    def get_all_works(self) -> List[Work]:
        pass

    @abstractmethod
    def get_all_magazines(self) -> List[Magazine]:
        pass

    @abstractmethod
    def get_author_by_id(self, author_id: str) -> Optional[Author]:
        pass

    @abstractmethod
    def get_work_by_id(self, work_id: str) -> Optional[Work]:
        pass

    @abstractmethod
    def get_magazine_by_id(self, magazine_id: str) -> Optional[Magazine]:
        pass
