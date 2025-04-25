import networkx as nx
from load_data import Loader, add_labels
import random

def print_largest_connected_component_size(G):
    if not G.is_directed():
        raise ValueError("Expected a directed graph for strongly connected component analysis.")

    components = nx.strongly_connected_components(G)
    largest_cc = max(components, key=len)
    print(f"\nSize of largest strongly connected component: {len(largest_cc)}")

def analyze_graph(G, vertices):
    """
    Call different analysis functions on the graph G.
    """
    print("************************************")
    print(f"Graph Analysis: |G| = {G.number_of_nodes()}, |E| = {G.number_of_edges()}")

    print_largest_connected_component_size(G)

    # Top nodes by in-degree
    in_degrees = sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 nodes by in-degree:")
    for node_id, degree in in_degrees:
        print(f"{vertices.get(node_id, node_id)}: {degree}")

    # Top nodes by PageRank
    pagerank = nx.pagerank(G, alpha=0.85)
    top_pr = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 nodes by PageRank:")
    for node_id, score in top_pr:
        print(f"{vertices.get(node_id, node_id)}: {score:.6f}")

def main():
    G_2025, vert_2025 = Loader("2025-jan-feb-mar")
    G_2024, vert_2024 = Loader("2024-oct-nov-dec")

    # add_labels(G, "data/cc-main-2025-jan-feb-mar-domain-vertices.txt", "data/domain_pc1.csv")

    analyze_graph(G_2025, vert_2025)

if __name__ == "__main__":
    main()