import gzip

from neo4j_viz import Node, Relationship, VisualizationGraph


def render_graph():
    MAX_NODES = 9000

    # Load the first 9000 hostnames
    with gzip.open("../data/vertices.txt.gz", "rt") as f:
        hostnames = [line.strip() for _, line in zip(range(MAX_NODES), f)]

    # Predefine allowed node IDs
    allowed_ids = set(range(MAX_NODES))

    # Load and filter edges
    relationships = []
    with gzip.open("../data/edges.txt.gz", "rt") as f:
        for line in f:
            src, dst = map(int, line.strip().split())
            if src in allowed_ids and dst in allowed_ids:
                relationships.append(
                    Relationship(source=src, target=dst, caption="links_to")
                )

    # Create nodes only for used node IDs
    used_node_ids = set()
    for rel in relationships:
        used_node_ids.add(rel.source)
        used_node_ids.add(rel.target)

    nodes = [Node(id=i, caption=hostnames[i]) for i in used_node_ids]

    graph = VisualizationGraph(nodes=nodes, relationships=relationships)
    return graph.render()


def render_graph_static():
    MAX_NODES = 9000

    # Load the first 9000 hostnames
    with gzip.open("data/vertices.txt.gz", "rt") as f:
        hostnames = [line.strip() for _, line in zip(range(MAX_NODES), f)]

    allowed_ids = set(range(MAX_NODES))

    # Filter edges where both src and dst are within first 9000
    relationships = []
    with gzip.open("data/edges.txt.gz", "rt") as f:
        for line in f:
            src, dst = map(int, line.strip().split())
            if src in allowed_ids and dst in allowed_ids:
                relationships.append(
                    Relationship(source=src, target=dst, caption="links_to")
                )

    # Create nodes only for those referenced by relationships
    used_node_ids = set()
    for rel in relationships:
        used_node_ids.update([rel.source, rel.target])

    nodes = [Node(id=i, caption=hostnames[i]) for i in used_node_ids]

    print(f"Saving {len(nodes)} nodes and {len(relationships)} edges to HTML...")

    # Create the graph
    graph = VisualizationGraph(nodes=nodes, relationships=relationships)

    # Save static HTML
    html_str = graph.to_html()
    with open("webgraph_visualization.html", "w", encoding="utf-8") as f:
        print("saving HTML...")
        f.write(html_str)


def main() -> None:
    render_graph_static()


if __name__ == "__main__":
    main()
