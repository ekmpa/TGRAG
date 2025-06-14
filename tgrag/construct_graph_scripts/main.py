import os
from typing import List

from tgrag.construct_graph_scripts.process_compressed_text import (
    move_and_rename_compressed_outputs,
)
from tgrag.construct_graph_scripts.temporal_merge import TemporalGraphMerger
from tgrag.utils.path import get_root_dir


def main(slices: List[str]) -> None:
    file_path = get_root_dir()
    move_and_rename_compressed_outputs(
        source_base=f'{file_path}/data/output_text_dir',
        target_base_root=f'{file_path}/data/output_text_dir',
    )

    base_path = f'{file_path}/data/crawl-data/'
    output_dir = os.path.join(base_path, 'temporal')

    merger = TemporalGraphMerger(output_dir)

    for slice_id in slices:
        vertices_path = os.path.join(
            f'{base_path}/{slice_id}/output_text_dir/', 'vertices.txt.gz'
        )
        edges_path = os.path.join(
            f'{base_path}/{slice_id}/output_text_dir/', 'edges.txt.gz'
        )

        if not (os.path.exists(vertices_path) and os.path.exists(edges_path)):
            print(f'Missing data for {slice_id}: Skipping')
            continue

        merger.add_graph(base_path, vertices_path, edges_path, slice_id)

    merger.save()
    merger.print_overlap()


if __name__ == '__main__':
    main(['CC-MAIN-2017-13'])
    # if len(sys.argv) < 2:
    #     print("Usage: main.py CC-MAIN-YYYY-NN [CC-MAIN-YYYY-NN ...]")
    #     sys.exit(1)
    # main(sys.argv[1:])
