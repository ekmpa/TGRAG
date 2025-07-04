import pandas as pd

from tgrag.utils.matching import extract_graph_domains


def load_credibility_scores(path: str, use_core: bool = False) -> pd.DataFrame:
    cred_df = pd.read_csv(path)
    cred_df['match_domain'] = cred_df['domain']
    return cred_df[['match_domain', 'pc1']]


def get_credibility_intersection(source_path: str, time_slice: str) -> None:
    # Adjust paths
    cred_scores_path = f'{source_path}/dqr/domain_pc1.csv'
    vertices_path = (
        f'{source_path}/crawl-data/{time_slice}/output_text_dir/vertices.txt.gz'
    )
    print(f'Opening vertices file: {vertices_path}')
    output_csv_path = f'{source_path}/crawl-data/{time_slice}/node_credibility.csv'

    cred_df = load_credibility_scores(cred_scores_path)

    vertices_df = extract_graph_domains(vertices_path)

    enriched_df = pd.merge(vertices_df, cred_df, on='match_domain', how='inner')
    enriched_df.to_csv(output_csv_path, index=False)

    print(f'INFO: Merge done. Annotated file saved to {output_csv_path}')

    # After loading
    graph_domains_set = set(vertices_df['match_domain'].unique())
    cred_labels_set = set(cred_df['match_domain'].unique())

    common = graph_domains_set.intersection(cred_labels_set)
    print(f'Number of common domains: {len(common)}')

    # Node annotation stats
    annotated_nodes = len(enriched_df)
    total_nodes = len(vertices_df)
    node_percentage = (annotated_nodes / total_nodes) * 100

    # Label coverage stats (truth labels matched at least once)
    matched_labels = len(cred_labels_set.intersection(graph_domains_set))
    total_labels = len(cred_labels_set)
    label_percentage = (matched_labels / total_labels) * 100

    print(
        f'{annotated_nodes} / {total_nodes} nodes annotated with credibility scores ({node_percentage:.2f}%).'
    )
    print(
        f'{matched_labels} / {total_labels} credibility labels matched at least once on the graph ({label_percentage:.2f}%).'
    )
