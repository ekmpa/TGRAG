import gzip
import os
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd


class Merger:
    """Parent class for merging two graphs.
    Used for temporal and article-level merging.
    """

    def __init__(self, output_dir: str) -> None:
        self.output_dir = output_dir
        self.edges: List[Tuple[int, int, int, str]] = []
        self.domain_to_node: Dict[str, Tuple[int, int]] = {}

    def _normalize_domain(self, raw: str) -> str:
        """Normalize domain strings for consistency across slices."""
        raw = raw.strip().lower()
        if '://' in raw:
            raw = urlparse(raw).hostname or raw
        if raw.startswith('www.'):
            raw = raw[4:]
        if ':' in raw:
            raw = raw.split(':')[0]
        if raw.endswith('.'):
            raw = raw[:-1]
        return raw

    def _load_vertices(self, filepath: str) -> Tuple[List[str], List[int]]:
        """Helper to extract and load vertices from vertices.txt.gz."""
        domains = []
        node_ids = []
        with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                parts = line.strip().split('\t')
                norm = self._normalize_domain(parts[1])
                domains.append(norm)
                node_ids.append(int(parts[0]))
        return domains, node_ids

    def _load_edges(self, filepath: str) -> List[Tuple[int, int]]:
        with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
            result: List[Tuple[int, int]] = []
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2:
                    result.append((int(parts[0]), int(parts[1])))
            return result

    def save(self) -> None:
        """Save merged graph to CSV."""
        os.makedirs(self.output_dir, exist_ok=True)

        # Ensure all edges have 4 fields, defaulting edge_type to 'hyperlinks'
        processed_edges = [
            edge if len(edge) == 4 else (*edge, 'hyperlinks') for edge in self.edges
        ]

        edge_cols = ['src', 'dst', 'time_id', 'edge_type']
        pd.DataFrame(processed_edges, columns=edge_cols).to_csv(
            os.path.join(self.output_dir, 'temporal_edges.csv'), index=False
        )

        df_nodes = pd.DataFrame(
            [
                {'domain': domain, 'node_id': node_id, 'time_id': time_id}
                for domain, (node_id, time_id) in self.domain_to_node.items()
            ]
        )
        df_nodes.to_csv(
            os.path.join(self.output_dir, 'temporal_nodes.csv'), index=False
        )
