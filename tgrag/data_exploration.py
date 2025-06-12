import gzip
import io
import os
from typing import Dict, Union

import pandas as pd
import requests
import tldextract


def fetch_crawl_metadata() -> Dict:
    url = 'https://index.commoncrawl.org/collinfo.json'
    try:
        response = requests.get(url)
        response.raise_for_status()
        metadata = response.json()
        crawl_info = {entry['id']: entry for entry in metadata}
        return crawl_info
    except Exception as e:
        print(f'Warning: Unable to fetch crawl metadata: {e}')
        return {}


def parse_properties(filepath: str) -> Dict:
    props = {}
    with open(filepath, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                props[key.strip()] = value.strip()
    return props


def extract_graph_domains(filepath: str, use_core: bool = True) -> pd.DataFrame:
    rows = []
    with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            line = line.strip().lower()
            if not line:
                continue
            domain_part = line.split('\t', 1)[-1]
            ext = tldextract.extract(domain_part)
            if use_core and ext.domain:
                domain = ext.domain
            elif ext.domain and ext.suffix:
                domain = f'{ext.domain}.{ext.suffix}'
            else:
                continue
            rows.append((i, domain))
    df = pd.DataFrame(rows, columns=['node_id', 'domain'])
    return df


def load_degrees(slice_name: str) -> Union[pd.DataFrame, None]:
    path = f'rank_output/{slice_name}/graph_{slice_name}.outdegree'
    if not os.path.isfile(path):
        print(f'Warning: Degree file not found for {slice_name}')
        return None
    try:
        df = pd.read_csv(path, header=None, names=['degree'])
        return df
    except Exception as e:
        print(f'Error loading degrees for {slice_name}: {e}')
        return None


def load_pagerank_txtgz(slice_name: str) -> Union[pd.DataFrame, None]:
    path = f'rank_output/{slice_name}/graph_{slice_name}-ranks.txt.gz'
    if not os.path.isfile(path):
        print(f'Warning: PageRank .txt.gz file not found for {slice_name}')
        return None
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            lines = []
            for line in f:
                if not line.lstrip().startswith('#'):
                    lines.append(line)
            if not lines:
                return None
            df = pd.read_csv(
                io.StringIO(''.join(lines)),
                sep='\t',
                header=None,
                names=['pagerank', 'node_id'],
            )
        return df
    except Exception as e:
        print(f'Error loading PageRank .txt.gz for {slice_name}: {e}')
        return None


def print_metadata_for_slice(slice_name: str, metadata_dict: dict) -> None:
    cc_id = slice_name.replace('MAIN_', 'CC-MAIN-').replace('_', '-')
    info = metadata_dict.get(cc_id)
    if info:
        print(f'Metadata for {slice_name}:')
        print(f"  Crawl: {info.get('name', 'N/A')}")
        print(f"  Common Crawl Index: {info.get('cdx-api', 'N/A')}")
    else:
        print(f'Metadata not found for {slice_name}')


def analyze_slice(slice_name: str, metadata_dict: dict) -> None:
    print(f'========== {slice_name} ==========')

    print_metadata_for_slice(slice_name, metadata_dict)

    # --- Graph Properties ---
    prop_file = f'rank_output/{slice_name}/graph_{slice_name}-t.properties'
    if not os.path.exists(prop_file):
        print(f'Warning: Properties file not found for {slice_name}')
        return

    props = parse_properties(prop_file)
    total_nodes = int(props.get('nodes', -1))
    print(f'Total Nodes: {total_nodes}')

    # --- Degree Info ---
    degrees_df = load_degrees(slice_name)
    if degrees_df is not None and not degrees_df.empty:
        num_edges = degrees_df['degree'].sum()
        avg_degree = degrees_df['degree'].mean()
        min_degree = degrees_df['degree'].min()
        max_degree = degrees_df['degree'].max()
        density = (
            num_edges / (total_nodes * (total_nodes - 1)) if total_nodes > 1 else 0
        )
        print(f'Total Edges: {num_edges}')
        print(f'Average Degree: {avg_degree:.2f}')
        print(f'Degree Range: min={min_degree}, max={max_degree}')
        print(f'Density: {density:.6f}')
    else:
        print(f'Warning: Degree data not available for {slice_name}')

    # --- PageRank ---
    pagerank_df = load_pagerank_txtgz(slice_name)
    if pagerank_df is not None and not pagerank_df.empty:
        top5 = pagerank_df.sort_values(by='pagerank', ascending=False).head(5)
        print('\nTop 5 PageRanks:')
        print(top5.to_string(index=False))
        print(
            f"PageRank Stats: min={pagerank_df['pagerank'].min():.6f}, max={pagerank_df['pagerank'].max():.6f}, mean={pagerank_df['pagerank'].mean():.6f}"
        )
    else:
        print(f'Warning: PageRank file is empty or not found for {slice_name}')

    # --- Node Features (Domains) ---
    vertices_path = f'external/cc-webgraph/{slice_name}/vertices.txt.gz'
    if os.path.exists(vertices_path):
        graph_df = extract_graph_domains(vertices_path)
        print('\nSample Node Features:')
        print(graph_df.sample(5, random_state=42).to_string(index=False))
    else:
        print(f'Warning: {vertices_path} not found!')

    # --- Credibility Labels ---
    cred_path = f'external/cc-webgraph/{slice_name}/node_credibility.csv'
    if os.path.exists(cred_path):
        cred_df = pd.read_csv(cred_path)
        print('\nSample Node Credibility Scores:')
        print(cred_df.sample(5, random_state=42).to_string(index=False))
    else:
        print(f'Warning: Node credibility file not found for {slice_name}')

    print()


def main() -> None:
    slices_dir = 'rank_output'
    slice_names = sorted(
        [
            d
            for d in os.listdir(slices_dir)
            if os.path.isdir(os.path.join(slices_dir, d)) and d.startswith('MAIN_')
        ]
    )

    print(f'Found {len(slice_names)} slices.\n')

    metadata_dict = fetch_crawl_metadata()

    for slice_name in slice_names:
        analyze_slice(slice_name, metadata_dict)


if __name__ == '__main__':
    main()
