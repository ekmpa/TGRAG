import gzip
import os
import sys
import pandas as pd
from urllib.parse import urlparse

# this scripts merges multiple CC-MAIN slices into a temporal graph
# and allows for continual addition of new slices 

class TemporalGraphMerger:
    """
    Merges multiple slices into a temporal graph (both edges and nodes are temporal). 
    Then saves the graph (CSV) and can continually add slices to it.
    """
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.edges = []
        self.domain_to_node = {}  # domain → {id, first_seen, last_seen}
        self.slice_node_sets = {} # used for overlap stats 
        self.next_node_id = 0 
        self.time_ids_seen = set() # to track seen slices 
        self._last_overlap = None  # snapshot for single-slice mode
        self._load_existing() # load existing graph if exists 

    def _load_existing(self):
        """ Reconstruct graph from existing CSVs """
        edges_path = os.path.join(self.output_dir, "temporal_edges.csv")
        nodes_path = os.path.join(self.output_dir, "temporal_nodes.csv")

        if os.path.exists(edges_path) and os.path.exists(nodes_path):
            df_edges = pd.read_csv(edges_path)
            df_nodes = pd.read_csv(nodes_path)

            self.edges = list(df_edges.itertuples(index=False, name=None))
            self.domain_to_node = {
                row["domain"]: {
                    "id": row["node_id"],
                    "first_seen": row["first_seen"],
                    "last_seen": row["last_seen"],
                }
                for _, row in df_nodes.iterrows()
            }

            self.next_node_id = max(d["id"] for d in self.domain_to_node.values()) + 1
            self.time_ids_seen = set(df_edges["time_id"])
            print(f"Loaded existing graph with {len(self.domain_to_node)} nodes and {len(self.edges)} edges")

    def _normalize_domain(self, raw):
        """ Normalize domain strings for consistency across slices """
        raw = raw.strip().lower()
        if "://" in raw:
            raw = urlparse(raw).hostname or raw
        if raw.startswith("www."):
            raw = raw[4:]
        if ':' in raw:
            raw = raw.split(':')[0]
        if raw.endswith("."):
            raw = raw[:-1]
        return raw

    def _load_vertices(self, filepath):
        """ Helper to extract and load vertices from vertices.txt.gz """
        domains = []
        with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split("\t")
                norm = self._normalize_domain(parts[1])
                domains.append(norm)
        return domains

    def _load_edges(self, filepath):
        """ Helper to extract and load edges from edges.txt.gz """
        with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
            return [tuple(map(int, line.strip().split())) for line in f if len(line.strip().split()) == 2]

    def _slice_to_time_id(self, slice_id):
        """ Yield timestamp from slice ID (current logic: CC-MAIN-YYYY-NN -> YYYYNN) """
        try:
            parts = slice_id.split("-")
            return int(parts[2]) * 100 + int(parts[3])
        except (IndexError, ValueError):
            raise ValueError(f"Invalid slice format: {slice_id}. Expected CC-MAIN-YYYY-NN.")

    def add_graph(self, vertices_path, edges_path, slice_id):
        """ Add new slice to the existing temporal graph"""
        time_id = self._slice_to_time_id(slice_id)
        if time_id in self.time_ids_seen:
            print(f"Skipping slice {slice_id}: time_id {time_id} already exists.")
            return

        # snapshot current node IDs before mutation
        existing_node_ids = {v["id"] for v in self.domain_to_node.values()}

        # load vertices and edges using local -> global mapping
        domains = self._load_vertices(vertices_path)
        local_to_global = {}
        new_node_ids = set()

        for local_id, domain in enumerate(domains):
            if domain not in self.domain_to_node:
                self.domain_to_node[domain] = {
                    "id": self.next_node_id,
                    "first_seen": time_id,
                    "last_seen": time_id,
                }
                self.next_node_id += 1
            else:
                self.domain_to_node[domain]["last_seen"] = time_id

            node_id = self.domain_to_node[domain]["id"]
            local_to_global[local_id] = node_id
            new_node_ids.add(node_id)

        edges = self._load_edges(edges_path)
        for src_local, dst_local in edges:
            src_global = local_to_global.get(src_local)
            dst_global = local_to_global.get(dst_local)
            if src_global is not None and dst_global is not None:
                self.edges.append((src_global, dst_global, time_id))

        self.slice_node_sets[slice_id] = new_node_ids
        self.time_ids_seen.add(time_id)

        print(f"Added slice {slice_id} (time_id {time_id}): {len(new_node_ids)} nodes, {len(edges)} edges")

        # store overlap with pre-existing graph if this is the only slice being added now
        if len(self.slice_node_sets) == 1:
            self._last_overlap = len(existing_node_ids & new_node_ids)

    def save(self):
        """ Save merged graph to CSV """
        os.makedirs(self.output_dir, exist_ok=True)

        pd.DataFrame(self.edges, columns=["src", "dst", "time_id"]).to_csv(
            os.path.join(self.output_dir, "temporal_edges.csv"), index=False)

        df_nodes = pd.DataFrame([
            {
                "domain": domain,
                "node_id": info["id"],
                "first_seen": info["first_seen"],
                "last_seen": info["last_seen"],
            }
            for domain, info in self.domain_to_node.items()
        ])
        df_nodes.to_csv(os.path.join(self.output_dir, "temporal_nodes.csv"), index=False)

    def print_overlap(self):
        """ Print overlap stats across all / added slices """
        all_sets = list(self.slice_node_sets.values())

        if not all_sets:
            return

        if len(all_sets) == 1 and self._last_overlap is not None:
            print(f"Nodes in common with existing graph: {self._last_overlap}")
        else:
            common = set.intersection(*all_sets)
            print(f"Nodes in common across all slices: {len(common)}")

def main(slices):
    base_path = "external/cc-webgraph"
    output_dir = os.path.join(base_path, "temporal")

    merger = TemporalGraphMerger(output_dir)

    for slice_id in slices:
        folder = slice_id.replace("-", "_")
        vertices_path = os.path.join(base_path, folder, "vertices.txt.gz")
        edges_path = os.path.join(base_path, folder, "edges.txt.gz")

        if not (os.path.exists(vertices_path) and os.path.exists(edges_path)):
            print(f"Missing data for {slice_id}: Skipping")
            continue

        merger.add_graph(vertices_path, edges_path, slice_id)

    merger.save()
    merger.print_overlap()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./merge_pipeline.sh CC-MAIN-YYYY-NN [CC-MAIN-YYYY-NN ...]")
        sys.exit(1)
    main(sys.argv[1:])