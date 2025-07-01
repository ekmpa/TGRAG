from pathlib import Path

import pytest
import torch
from pytest import approx

from tgrag.encoders.rni_encoding import RNIEncoder
from tgrag.encoders.time_encoder import TimeStampEncoder
from tgrag.utils.data_loading import load_edge_csv, load_node_csv


@pytest.fixture
def node_csv(tmp_path: Path):
    content = """domain,node_id,time_id
ie.pdconstruction,84505,20250525
ie.peikko,84506,20250525
ie.pembrokewanderers,84507,20250525
"""
    file = tmp_path / "nodes.csv"
    file.write_text(content)
    return str(file)


@pytest.fixture
def edge_csv(tmp_path: Path):
    content = """src,dst,time_id
84505,34388,20250525
84505,34493,20250525
84505,37959,20250525
"""
    file = tmp_path / "edges.csv"
    file.write_text(content)
    return str(file)


def test_load_node_csv_with_rni_encoders(node_csv):

    encoders = {
        "random": RNIEncoder(10),
    }

    x, mapping = load_node_csv(node_csv, index_col=1, encoders=encoders)
    print(x[0])
    assert isinstance(x, torch.Tensor)
    assert x.shape[0] == 3
    assert mapping[84505] == 0
    assert approx(x[0].item(), rel=1e-1) == -0.6014


def test_load_node_csv_with_encoders(node_csv):

    encoders = {
        "random": RNIEncoder(10),
        "time_id": TimeStampEncoder(),
    }

    x, mapping = load_node_csv(node_csv, index_col=1, encoders=encoders)
    assert isinstance(x, torch.Tensor)
    assert x.shape[0] == 6
    assert mapping[84505] == 0


def test_load_edge_csv(edge_csv):
    edge_index, edge_attrs = load_edge_csv(
        edge_csv, src_index_col="src", dst_index_col="dst", encoders=None
    )

    assert isinstance(edge_index, torch.Tensor)
    assert edge_index.shape == torch.Size([2, 3])
    assert edge_attrs is None
