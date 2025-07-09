from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime


@dataclass
class Author:
    id: str
    name: str
    birth_date: Optional[datetime] = None
    biography: Optional[str] = None


@dataclass
class Magazine:
    id: str
    name: str
    publisher: str
    established_date: Optional[datetime] = None


@dataclass
class Work:
    id: str
    title: str
    authors: List[Author]
    magazines: List[Magazine]
    publication_date: Optional[datetime] = None
    genre: Optional[str] = None
    description: Optional[str] = None
    isbn: Optional[str] = None
    cover_image_url: Optional[str] = None


@dataclass
class GraphNode:
    id: str
    label: str
    type: str
    properties: dict


@dataclass
class GraphEdge:
    id: str
    source: str
    target: str
    type: str
    properties: Optional[dict] = None
