from typing import Dict, List, Tuple


class Merger:
    """Parent class for merging two graphs.
    Used for temporal and article-level merging.
    """

    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.edges: List[Tuple[int, int, int]] = []
        self.domain_to_node: Dict[str, Tuple[int, int]] = {}
