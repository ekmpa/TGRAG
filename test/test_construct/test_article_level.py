import gzip
from pathlib import Path

import pytest

from tgrag.utils.merger import Merger


@pytest.fixture
def merger(tmp_path: Path) -> Merger:
    return Merger(output_dir=str(tmp_path))


@pytest.fixture
def example_vertices(tmp_path: Path) -> str:
    file_path = tmp_path / 'vertices.txt.gz'
    lines = ['1\tcom.example', '2\twww.test.co.uk', '3\tinvalid_domain']
    with gzip.open(file_path, 'wt', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')
    return str(file_path)


@pytest.fixture
def example_edges(tmp_path: Path) -> str:
    file_path = tmp_path / 'edges.txt.gz'
    lines = [
        '1 2',
        '2 3',
    ]
    with gzip.open(file_path, 'wt', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')
    return str(file_path)
