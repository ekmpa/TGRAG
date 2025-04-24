import networkx as nx
import os
import random


# Edge loading functions
# =========================================================

def load_edges(file_path, max_edges=100000):
    """
    Load up to `max_edges` from the edge file.
    Returns a list of edges and a set of node IDs used.
    """
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

def load_edges_prioritized(edge_path, domain_to_id, labels, max_edges=100000):
    """
    Load up to `max_edges`, only including edges that involve labeled domains.
    """
    label_node_ids = set()
    for domain in labels:
        domain_norm = domain.lower().strip().replace("www.", "")
        node_id = domain_to_id.get(domain_norm)
        if node_id is not None:
            label_node_ids.add(node_id)

    edges = []
    used_nodes = set()

    with open(edge_path, 'r') as f:
        for line in f:
            if len(edges) >= max_edges:
                break
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            src = int(parts[0])
            tgt = int(parts[1])
            if src in label_node_ids or tgt in label_node_ids:
                edges.append((src, tgt))
                used_nodes.update([src, tgt])

    print(f"INFO: Prioritized loading {len(edges)} edges involving labeled domains")
    return edges, used_nodes

def load_edges_labeled_plus_random(edge_path, domain_to_id, labels, max_edges=100000, min_labeled_edges=2000):
    """
    Load a mix of labeled and random edges.
    Labeled edges are prioritized, and random edges are sampled from the rest.
    """
    label_node_ids = set()
    for domain in labels:
        domain_norm = domain.lower().strip().replace("www.", "")
        node_id = domain_to_id.get(domain_norm)
        if node_id is not None:
            label_node_ids.add(node_id)

    labeled_edges = []
    labeled_nodes = set()
    reservoir = []  # For random edges

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

    print(f"INFO: Loaded {len(final_edges)} edges ({len(labeled_edges)} labeled, {num_random} random)")
    return final_edges, final_nodes

# Vertex loading functions
# =========================================================

def load_vertices(file_path, node_ids_to_keep):
    """
    Load only vertices that are present in `node_ids_to_keep`.
    """
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
def build_graph(edges):
    G = nx.DiGraph()
    G.add_edges_from(edges)
    return G

def Loader():
    base_dir = "data/"
    vertices_path = os.path.join(base_dir, "cc-main-2025-jan-feb-mar-domain-vertices.txt")
    edges_path = os.path.join(base_dir, "cc-main-2025-jan-feb-mar-domain-edges.txt")
    label_path = os.path.join(base_dir, "domain_pc1.csv")

    print("INFO:Loading labels...")
    labels = load_labels(label_path)
    domain_to_id = map_domains_to_ids(vertices_path)

    print("INFO:Loading labeled + random edges...")
    edges, node_ids = load_edges_labeled_plus_random(
        edge_path=edges_path,
        domain_to_id=domain_to_id,
        labels=labels,
        max_edges=100_000,
        min_labeled_edges=2000
    )

    print(f"INFO:Loaded {len(edges)} edges using {len(node_ids)} unique nodes")

    print("INFO:Loading relevant vertices...")
    vertices = load_vertices(vertices_path, node_ids)
    print(f"INFO:Loaded {len(vertices)} vertices")

    print("INFO:Building graph...")
    G = build_graph(edges)

    print("INFO:Attaching labels...")
    node_labels = align_labels_with_graph(labels, domain_to_id)
    count = 0
    for node_id, score in node_labels.items():
        if G.has_node(node_id):
            G.nodes[node_id]["label"] = score
            count += 1
    print(f"INFO: Added labels to {count} nodes")

    return G, vertices

# Label loading functions
# =========================================================

def load_labels(label_file_path):
    """
    Load credibility labels from CSV with header: domain,pc1
    """
    labels = {}
    with open(label_file_path, 'r') as f:
        next(f)  # skip header
        for line in f:
            domain, score = line.strip().split(',')
            labels[domain.strip()] = float(score)

    return labels


def map_domains_to_ids(vertices_path):
    """
    Load domain -> node_id mapping from Common Crawl domain-vertices file
    """
    domain_to_id = {}
    with open(vertices_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                node_id = int(parts[0])
                domain = parts[1].lower().strip()
                if domain.startswith("www."):
                    domain = domain[4:]
                domain_to_id[domain] = node_id
                domain_to_id[domain] = node_id
    return domain_to_id

def align_labels_with_graph(labels, domain_to_id):
    node_labels = {}
    normalized_domain_to_id = {domain.lower().strip(): nid for domain, nid in domain_to_id.items()}
    for domain, score in labels.items():
        domain_key = domain.lower().strip()
        node_id = normalized_domain_to_id.get(domain_key)
        if node_id is not None:
            node_labels[node_id] = score
    return node_labels

def add_labels(G, vertices_path, label_path):
    """
    Add credibility labels as node attributes to a graph G.
    """
    labels = load_labels(label_path)
    domain_to_id = map_domains_to_ids(vertices_path)
    node_labels = align_labels_with_graph(labels, domain_to_id)

    count = 0
    for node_id, score in node_labels.items():
        if G.has_node(node_id):
            G.nodes[node_id]["label"] = score
            count += 1

    print(f"INFO: Added labels to {count} nodes in the graph")