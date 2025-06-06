import gzip
import os
import pandas as pd

class TemporalGraphMerger:
    def __init__(self):
        self.domain_to_node = {}
        self.edges = []
        self.next_node_id = 0
        self.current_time_id = 0
        self.slice_stats = []

    def _load_vertices(self, filepath):
        domain_list = []
        with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                domain = line.strip().lower()
                if domain:
                    domain_list.append(domain)
        return domain_list

    def _load_edges(self, filepath):
        edges = []
        with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2:
                    src, dst = map(int, parts)
                    edges.append((src, dst))
        return edges

    def add_graph(self, vertices_path, edges_path, slice_id):
        """Add one slice (graph) to the temporal graph."""
        domains = self._load_vertices(vertices_path)
        num_nodes = len(domains)

        local_to_global = {}
        for local_id, domain in enumerate(domains):
            if domain not in self.domain_to_node:
                self.domain_to_node[domain] = self.next_node_id
                self.next_node_id += 1
            local_to_global[local_id] = self.domain_to_node[domain]

        edges = self._load_edges(edges_path)
        num_edges = len(edges)

        for src_local, dst_local in edges:
            src_global = local_to_global.get(src_local)
            dst_global = local_to_global.get(dst_local)
            if src_global is not None and dst_global is not None:
                self.edges.append((src_global, dst_global, self.current_time_id))

        self.slice_stats.append({
            "slice_id": slice_id,
            "domains_set": set(domains),
            "num_nodes": num_nodes,
            "num_edges": num_edges
        })

        self.current_time_id += 1

    def save(self, output_dir):
        """Save the temporal graph in a specified folder."""
        os.makedirs(output_dir, exist_ok=True)

        output_edges_path = os.path.join(output_dir, "temporal_edges.csv")
        output_nodes_path = os.path.join(output_dir, "temporal_nodes.csv")

        df_edges = pd.DataFrame(self.edges, columns=["src", "dst", "time_id"])
        df_edges.to_csv(output_edges_path, index=False)

        df_nodes = pd.DataFrame.from_dict(self.domain_to_node, orient="index", columns=["node_id"])
        df_nodes.index.name = "domain"
        df_nodes.reset_index().to_csv(output_nodes_path, index=False)

        print(f"\nSaved merged edges to {output_edges_path}")
        print(f"Saved merged nodes to {output_nodes_path}")

    def print_stats(self):
        """Print statistics about the merge."""
        print("\n=== Individual Slice Stats ===")
        for stat in self.slice_stats:
            print(f"Slice {stat['slice_id']}: {stat['num_nodes']} nodes, {stat['num_edges']} edges")

        print("\n=== Merged Graph Stats ===")
        all_domains = [stat["domains_set"] for stat in self.slice_stats]
        total_unique_domains = set.union(*all_domains)
        print(f"Total unique nodes: {len(total_unique_domains)}")
        print(f"Total edges (with time info): {len(self.edges)}")

        if len(all_domains) > 1:
            common_domains = set.intersection(*all_domains)
            print(f"Nodes in common across all slices: {len(common_domains)}")

def main(
    slices,
    base_path="external/cc-webgraph",
    output_dir="external/cc-webgraph/temporal"
):
    merger = TemporalGraphMerger()

    for slice_id in slices:
        folder_name = slice_id.replace("-", "_")
        vertices_path = os.path.join(base_path, folder_name, "vertices.txt.gz")
        edges_path = os.path.join(base_path, folder_name, "edges.txt.gz")

        print(f"Adding slice {slice_id} with vertices {vertices_path} and edges {edges_path}")
        merger.add_graph(vertices_path, edges_path, slice_id)

    merger.save(output_dir)
    merger.print_stats()

if __name__ == "__main__":
    slices = ["CC-MAIN-2014-10", "CC-MAIN-2014-15"]
    main(slices)