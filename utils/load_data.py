import os
import random
from typing import Dict, List, Set, Tuple

import networkx as nx


def load_edges(
    file_path: str, max_edges: int = 100000
) -> Tuple[List[Tuple[int, int]], Set[int]]:
    """Load up to `max_edges` from the edge file at random."""
    edges = []
    used_nodes = set()
    with open(file_path, 'r') as f:
        for i, line in enumerate(f):
            if i >= max_edges:
                break
            parts = line.strip().split('\t')
            if len(parts) == 2:
                source = int(parts[0])
                target = int(parts[1])
                edges.append((source, target))
                used_nodes.update([source, target])
    return edges, used_nodes


def load_edges_labeled_plus_random(
    edge_path: str,
    domain_to_id: Dict,
    labels: Dict,
    max_edges: int = 100000,
) -> Tuple[List[Tuple[int, int]], Set[int]]:
    """Load a mix of labeled and random edges.
    Labeled edges are prioritized, and random edges are sampled from the rest.
    """
    label_node_ids = set()

    for domain in labels:
        domain_norm = domain.lower().strip().replace('www.', '')
        node_id = domain_to_id.get(domain_norm)
        if node_id is not None:
            label_node_ids.add(node_id)

    labeled_edges = []
    labeled_nodes = set()
    reservoir: List[Tuple[int, int]] = []  # For random edges

    with open(edge_path, 'r') as f:
        for i, line in enumerate(f):
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            src, tgt = int(parts[0]), int(parts[1])
            edge = (src, tgt)

            if src in label_node_ids or tgt in label_node_ids:
                labeled_edges.append(edge)
                labeled_nodes.update([src, tgt])
            else:
                # Reservoir sampling: keep only a fixed-size buffer
                if len(reservoir) < max_edges:
                    reservoir.append(edge)
                else:
                    j = random.randint(0, i)
                    if j < max_edges:
                        reservoir[j] = edge

    # How many random edges to keep
    num_random = max_edges - len(labeled_edges)
    if num_random < 0:
        labeled_edges = labeled_edges[:max_edges]
        num_random = 0

    final_edges = labeled_edges + reservoir[:num_random]
    final_nodes = set()
    for src, tgt in final_edges:
        final_nodes.update([src, tgt])

    print(
        f'--INFO: Loaded {len(final_edges)} edges ({len(labeled_edges)} labeled, {num_random} random)'
    )
    return final_edges, final_nodes


# Vertex loading functions
# =========================================================
def load_vertices(file_path: str, node_ids_to_keep: Set) -> Dict:
    """Load only vertices that are present in `node_ids_to_keep`."""
    vertices = {}
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                node_id = int(parts[0])
                if node_id in node_ids_to_keep:
                    domain = parts[1]
                    vertices[node_id] = domain
    return vertices


# Graph building functions
# =========================================================
def build_graph(edges: List[Tuple[int, int]]) -> nx.DiGraph:
    G = nx.DiGraph()
    G.add_edges_from(edges)
    return G
    # return G, vertices


# Label loading functions
# =========================================================


def load_labels(label_file_path: str) -> Dict:
    """Load credibility labels from CSV with header: domain,pc1."""
    labels = {}
    with open(label_file_path, 'r') as f:
        next(f)  # skip header
        for line in f:
            domain, score = line.strip().split(',')
            labels[domain.strip()] = float(score)

    return labels


def map_domains_to_ids(vertices_path: str) -> Dict:
    """Load domain -> node_id mapping from Common Crawl domain-vertices file."""
    domain_to_id = {}
    with open(vertices_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                node_id = int(parts[0])
                domain = parts[1].lower().strip()
                if domain.startswith('www.'):
                    domain = domain[4:]
                domain_to_id[domain] = node_id
                domain_to_id[domain] = node_id
    return domain_to_id


def align_labels_with_graph(labels: Dict, domain_to_id: Dict) -> Dict:
    """Align labels with the graph by normalizing domain names and matching them to node IDs."""
    node_labels = {}
    normalized_domain_to_id = {
        domain.lower().strip(): nid for domain, nid in domain_to_id.items()
    }
    for domain, score in labels.items():
        domain_key = domain.lower().strip()
        node_id = normalized_domain_to_id.get(domain_key)
        if node_id is not None:
            node_labels[node_id] = score
    return node_labels


def add_labels(G: nx.DiGraph, vertices_path: str, label_path: str) -> None:
    """Add credibility labels as node attributes to G."""
    labels = load_labels(label_path)
    domain_to_id = map_domains_to_ids(vertices_path)
    node_labels = align_labels_with_graph(labels, domain_to_id)

    count = 0
    for node_id, score in node_labels.items():
        if G.has_node(node_id):
            G.nodes[node_id]['label'] = score
            count += 1

    print(f'INFO: Added labels to {count} nodes in the graph')


# Main function
# =========================================================


def Loader(period: str) -> Tuple[nx.DiGraph, Dict]:
    base_dir = 'data/'
    vert_path = os.path.join(base_dir, f'cc-main-{period}-domain-vertices.txt')
    edges_path = os.path.join(base_dir, f'cc-main-{period}-domain-edges.txt')
    label_path = os.path.join(base_dir, 'domain_pc1.csv')

    labels = load_labels(label_path)
    domain_to_id = map_domains_to_ids(vert_path)
    print('INFO:Loaded labels for', len(labels), 'domains')

    edges, node_ids = load_edges_labeled_plus_random(
        edge_path=edges_path,
        domain_to_id=domain_to_id,
        labels=labels,
        max_edges=50_000,
    )

    vertices = load_vertices(vert_path, node_ids)

    print(
        f'INFO:Loaded {len(edges)} edges (labeled+random) and {len(vertices)} relevant vertices'
    )

    G = build_graph(edges)
    print('--INFO:G built from Common Crawl + domains successfully.')
    return G, vertices
