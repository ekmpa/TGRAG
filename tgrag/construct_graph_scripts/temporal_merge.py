import gzip
import os
import re
from glob import glob
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pandas as pd

from tgrag.utils.merger import Merger

# this scripts merges multiple CC-MAIN slices into a temporal graph
# and allows for continual addition of new slices


class TemporalGraphMerger(Merger):
    """Merges multiple slices into a temporal graph (both edges and nodes are temporal).
    Then saves the graph (CSV) and can continually add slices to it.
    """

    def __init__(self, output_dir: str) -> None:
        # self.output_dir: str = output_dir
        # self.edges: List[Tuple[int, int, int]] = []  # (src, dst, time_id)
        # self.domain_to_node: Dict[str, Tuple[int, int]] = {}  # domain → node_id
        super().__init__(output_dir)

        self.slice_node_sets: Dict[str, Set[int]] = {}  # slice_id → set of node_ids
        self.next_node_id: int = 0
        self.time_ids_seen: Set[int] = set()
        self._last_overlap: Optional[int] = None
        self._load_existing()

    def _load_existing(self) -> None:
        """Reconstruct graph from existing CSVs."""
        next_edges_path = os.path.join(self.output_dir, 'temporal_edges.csv')
        nodes_path = os.path.join(self.output_dir, 'temporal_nodes.csv')

        if os.path.exists(next_edges_path) and os.path.exists(nodes_path):
            try:
                df_edges = pd.read_csv(next_edges_path)
                df_nodes = pd.read_csv(nodes_path)
                self.edges = list(df_edges.itertuples(index=False, name=None))
                self.domain_to_node = {
                    row['domain']: (row['node_id'], -1)
                    for _, row in df_nodes.iterrows()
                }
                self.time_ids_seen = set(df_edges['time_id'])
                print(
                    f'Loaded existing graph with {len(self.domain_to_node)} nodes and {len(self.edges)} edges'
                )
            except Exception as e:
                print(
                    f'Error occured in reading csv, they are likely empty. Error: {e}'
                )
                print('Continuing without loading existing CSVs.')

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

    def _slice_to_time_id(self, next_root_path: str, slice_id: str) -> int:
        """Yield timestamp from slice ID (current logic: YYYYMMDD)."""
        pattern = os.path.join(next_root_path, slice_id, 'segments', '*', 'wat')
        wat_dir = glob(pattern)[0]
        wat_files = sorted(glob(os.path.join(wat_dir, '*.wat.gz')))
        warc_date_re = re.compile(r'WARC-Date:\s*(\d{4})-(\d{2})-(\d{2})')

        for path in wat_files:
            try:
                with gzip.open(path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        match = warc_date_re.search(line)
                        if match:
                            yyyy, mm, dd = match.groups()
                            return int(f'{yyyy}{mm}{dd}')
            except Exception as e:
                print(f'Warning: failed to parse {path}: {e}')
                continue

        raise ValueError(
            f'Could not extract scrape date from any WAT files in {wat_dir}'
        )

    def add_graph(
        self,
        next_root_path: str,
        next_vertices_path: str,
        next_edges_path: str,
        slice_id: str,
    ) -> None:
        """Add new slice to the existing temporal graph."""
        time_id = self._slice_to_time_id(next_root_path, slice_id)
        if time_id in self.time_ids_seen:
            print(f'Skipping slice {slice_id}: time_id {time_id} already exists.')
            return

        # snapshot current node IDs before mutation
        existing_node_ids = set(self.domain_to_node.values())

        # load vertices and edges using local -> global mapping
        domains, node_ids = self._load_vertices(next_vertices_path)
        new_node_ids = set()

        for local_id, domain in enumerate(domains):
            if domain not in self.domain_to_node:
                new_node_ids.add(node_ids[local_id])
            self.domain_to_node[domain] = (node_ids[local_id], time_id)

        edges = self._load_edges(next_edges_path)
        for src_local, dst_local in edges:
            self.edges.append((src_local, dst_local, time_id))

        self.slice_node_sets[slice_id] = new_node_ids
        self.time_ids_seen.add(time_id)

        print(
            f'Added slice {slice_id} (timestamp {time_id}): {len(new_node_ids)} nodes, {len(edges)} edges'
        )

        # store overlap with pre-existing graph if this is the only slice being added now
        if len(self.slice_node_sets) == 1:
            self._last_overlap = len(existing_node_ids & new_node_ids)

    def save(self) -> None:
        """Save merged graph to CSV."""
        os.makedirs(self.output_dir, exist_ok=True)

        pd.DataFrame(self.edges, columns=['src', 'dst', 'time_id']).to_csv(
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

    def print_overlap(self) -> None:
        """Print overlap stats across all / added slices."""
        all_sets = list(self.slice_node_sets.values())

        if not all_sets:
            return

        if len(all_sets) == 1 and self._last_overlap is not None:
            print(f'Nodes in common with existing graph: {self._last_overlap}')
        else:
            common = set.intersection(*all_sets)
            print(f'Nodes in common across all slices: {len(common)}')
