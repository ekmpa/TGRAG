from tgrag.construct_graph_scripts.load_labels import (
    get_credibility_intersection,
)
from tgrag.construct_graph_scripts.process_compressed_text import (
    create_csv_edges_vertices,
    move_and_rename_compressed_outputs,
)
from utils.path import get_root_dir


def main() -> None:
    file_path = get_root_dir()
    move_and_rename_compressed_outputs(
        source_base=f'{file_path}/data/output_text_dir',
        target_base_root=f'{file_path}/data/output_text_dir',
    )
    create_csv_edges_vertices(source_file=f'{file_path}/data/output_text_dir')

    get_credibility_intersection(source_path=f'{file_path}/data')


if __name__ == '__main__':
    main()
