import networkx as nx
from load_data import Loader, add_labels
import pickle

def analyze_graph(G, vertices):
    """
    Call different analysis functions on the graph G.
    """
    print("************************************")
    print(f"Graph Analysis: |G| = {G.number_of_nodes()}, |E| = {G.number_of_edges()}")

    ## ...
    
def main():
    G_2025, vert_2025 = Loader("2025-jan-feb-mar")
    G_2024, vert_2024 = Loader("2024-oct-nov-dec")

    for obj, path in [
        (G_2025, "saved_graphs/G_2025.gpickle"),
        (G_2024, "saved_graphs/G_2024.gpickle"),
        (vert_2025, "saved_graphs/vert_2025.pkl"),
        (vert_2024, "saved_graphs/vert_2024.pkl")
    ]: 
        with open(path, "wb") as f:
            pickle.dump(obj, f)
    
    analyze_graph(G_2025, vert_2025)

if __name__ == "__main__":
    main()