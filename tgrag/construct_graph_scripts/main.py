from tgrag.construct_graph_scripts.process_compressed_text import (
    move_and_rename_compressed_outputs,
)
from utils.path import get_root_dir


def main() -> None:
    file_path = get_root_dir()
    print(file_path)
    move_and_rename_compressed_outputs(source_base='')


if __name__ == '__main__':
    main()
