import gzip

import pandas as pd
import tldextract


def load_credibility_scores(path: str, use_core: bool = False) -> pd.DataFrame:
    cred_df = pd.read_csv(path)
    cred_df['domain'] = (
        cred_df['domain'].str.strip().str.lower().str.replace(r'^www\.', '', regex=True)
    )

    if use_core:
        cred_df['match_domain'] = cred_df['domain'].apply(
            lambda d: tldextract.extract(d).domain
        )
    else:
        cred_df['match_domain'] = cred_df['domain'].apply(
            lambda d: f'{tldextract.extract(d).domain}.{tldextract.extract(d).suffix}'
        )

    return cred_df[['match_domain', 'pc1']]


def extract_graph_domains(filepath: str, use_core: bool = False) -> pd.DataFrame:
    parsed = []
    with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            line = line.strip().lower()
            if not line:
                continue

            # TODO: There is a glaring error here,you are matching by name which is not correct. (e.g reuters.com != www.reuterscompany.com)
            domain_part = line.split('\t', 1)[-1]
            ext = tldextract.extract(domain_part)

            if use_core and ext.domain:
                parsed.append((i, ext.domain))
            elif ext.domain and ext.suffix:
                parsed.append((i, f'{ext.domain}.{ext.suffix}'))

    return pd.DataFrame(parsed, columns=['node_id', 'match_domain'])


def get_credibility_intersection(source_path: str) -> None:
    # Read the slice from env variable
    use_core = True

    # Adjust paths
    cred_scores_path = f'{source_path}/dqr/domain_pc1.csv'
    vertices_path = f'{source_path}/output_text_dir/vertices.txt.gz'
    print(f'Opening vertices file: {vertices_path}')
    output_csv_path = f'{source_path}/output_text_dir/node_credibility.csv'

    cred_df = load_credibility_scores(cred_scores_path, use_core=use_core)
    print(f'INFO: Loaded credibility scores, use_core = {use_core}')

    vertices_df = extract_graph_domains(vertices_path, use_core=use_core)

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
