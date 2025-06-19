import os
import pathlib
import sys
from typing import List

import pandas as pd

from tgrag.construct_graph_scripts.process_compressed_text import (
    move_and_rename_compressed_outputs,
)
from tgrag.construct_graph_scripts.subnetwork_construct import (
    construct_subnetwork,
)
from tgrag.construct_graph_scripts.temporal_merge import TemporalGraphMerger
from tgrag.utils.path import get_root_dir


def main(slices: List[str]) -> None:
    base_path = get_root_dir()

    crawl_path = f'{base_path}/data/crawl-data/'
    output_dir = os.path.join(crawl_path, 'temporal')

    merger = TemporalGraphMerger(output_dir)

    for slice_id in slices:

        move_and_rename_compressed_outputs(
            source_base=f'{base_path}/data/crawl-data/{slice_id}/output_text_dir',
            target_base_root=f'{base_path}/data/crawl-data/{slice_id}/output_text_dir',
        )

        vertices_path = os.path.join(
            f'{crawl_path}/{slice_id}/output_text_dir/', 'vertices.txt.gz'
        )
        edges_path = os.path.join(
            f'{crawl_path}/{slice_id}/output_text_dir/', 'edges.txt.gz'
        )

        if not (os.path.exists(vertices_path) and os.path.exists(edges_path)):
            print(f'Missing data for {slice_id}: Skipping')
            continue

        merger.add_graph(crawl_path, vertices_path, edges_path, slice_id)

    merger.save()
    merger.print_overlap()

    dqr_path = f'{base_path}/data/dqr/domain_pc1.csv'
    temporal_path = f'{base_path}/data/crawl-data/temporal'
    output_path = f'{base_path}/data/crawl-data/sub-networks/'
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
    temporal_edges_df = pd.read_csv(f'{temporal_path}/temporal_edges.csv')
    temporal_vertices_df = pd.read_csv(f'{temporal_path}/temporal_nodes.csv')
    construct_subnetwork(dqr_path, output_path, temporal_edges_df, temporal_vertices_df)


if __name__ == '__main__':
    #main(['CC-MAIN-2024-10'])
    if len(sys.argv) < 2:
        print('Usage: main.py CC-MAIN-YYYY-NN [CC-MAIN-YYYY-NN ...]')
        sys.exit(1)
    main(sys.argv[1:])
