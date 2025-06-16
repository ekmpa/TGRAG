import os
import re

import pandas as pd
from tqdm import tqdm

from tgrag.utils.path import get_root_dir


def contains_base_domain(input: str, base_domain: str) -> bool:
    forward_pat = rf'\b(?:[\w-]+\.)*{re.escape(base_domain)}\b'

    reversed_base = '.'.join(base_domain.split('.')[::-1])  # nasa.gov -> gov.nasa
    reverse_pat = rf'\b{re.escape(reversed_base)}(?:\.[\w-]+)*\b'

    combined_pat = rf'(?:{forward_pat}|{reverse_pat})'

    return re.search(combined_pat, input) is not None


def construct_subnetwork(
    dqr_path: str,
    output_path: str,
    temporal_edges: pd.DataFrame,
    temporal_vertices: pd.DataFrame,
) -> None:
    df_dqr = pd.read_csv(dqr_path)

    for base_domain, pc1 in tqdm(
        zip(df_dqr['domain'], df_dqr['pc1']), desc='Domain pc1 data'
    ):
        matched_vertices = temporal_vertices[
            temporal_vertices['domain'].apply(
                lambda d: contains_base_domain(d, base_domain)
            )
        ]

        if matched_vertices.empty:
            continue

        matched_node_ids = set(matched_vertices['node_id'])

        matched_edges = temporal_edges[
            temporal_edges['src'].isin(matched_node_ids)
            | temporal_edges['dst'].isin(matched_node_ids)
        ]

        sub_vertices_df = matched_vertices.copy()
        sub_edges_df = matched_edges.copy()

        dst_edge_node_ids = set()
        for _, row in matched_edges.iterrows():
            if row['src'] in matched_node_ids and row['dst'] not in matched_node_ids:
                dst_edge_node_ids.add(row['dst'])
            elif row['dst'] in matched_node_ids and row['src'] not in matched_node_ids:
                dst_edge_node_ids.add(row['src'])

        additional_vertices = temporal_vertices[
            temporal_vertices['node_id'].isin(dst_edge_node_ids)
        ]

        sub_vertices_df = pd.concat(
            [sub_vertices_df, additional_vertices], ignore_index=True
        )
        sub_path = f'{output_path}/sub_vertices_domain_{base_domain}'
        os.makedirs(sub_path, exist_ok=True)
        sub_vertices_df.to_csv(f'{sub_path}/vertices.csv', index=False)
        sub_edges_df.to_csv(f'{sub_path}/edges.csv', index=False)


def main() -> None:
    base_path = get_root_dir()
    dqr_path = f'{base_path}/data/dqr/domain_pc1.csv'
    temporal_path = f'{base_path}/data/crawl-data/temporal'
    output_path = f'{base_path}/data/crawl-data/sub-networks/'
    os.makedirs(output_path, exist_ok=True)
    temporal_edges_df = pd.read_csv(f'{temporal_path}/temporal_edges.csv')
    temporal_vertices_df = pd.read_csv(f'{temporal_path}/temporal_nodes.csv')
    construct_subnetwork(dqr_path, output_path, temporal_edges_df, temporal_vertices_df)


if __name__ == '__main__':
    main()
