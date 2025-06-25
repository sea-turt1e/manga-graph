from typing import Dict, List, Optional
from domain.repositories import MangaRepository
from domain.entities import GraphNode, GraphEdge


class SearchMangaUseCase:
    def __init__(self, manga_repository: MangaRepository):
        self.manga_repository = manga_repository
    
    def execute(
        self, 
        query: str, 
        depth: int = 2,
        node_types: Optional[List[str]] = None,
        edge_types: Optional[List[str]] = None
    ) -> Dict[str, List]:
        """
        Execute manga search and return graph data
        """
        if not query.strip():
            return {"nodes": [], "edges": []}
        
        # Get graph data from repository
        graph_data = self.manga_repository.search_graph(query, depth)
        
        # Filter by node types if specified
        if node_types:
            filtered_nodes = [
                node for node in graph_data["nodes"] 
                if node["type"] in node_types
            ]
            graph_data["nodes"] = filtered_nodes
        
        # Filter by edge types if specified
        if edge_types:
            filtered_edges = [
                edge for edge in graph_data["edges"] 
                if edge["type"] in edge_types
            ]
            graph_data["edges"] = filtered_edges
        
        return graph_data