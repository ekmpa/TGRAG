from pathlib import Path

import pandas as pd
import pytest
import torch

from tgrag.encoders.time_encoder import TimeStampEncoder


@pytest.fixture
def df_csv(tmp_path: Path):
    content = """domain,node_id,time_id
ie.pdconstruction,84505,20250525
ie.peikko,84506,20250525
ie.pembrokewanderers,84507,20250525
"""
    file = tmp_path / "nodes.csv"
    file.write_text(content)
    df = pd.read_csv(file)
    return df


def test_timeStampe_call(df_csv):
    tse = TimeStampEncoder()
    x = tse(df_csv['time_id'])
    print(x)
    assert isinstance(x, torch.Tensor)
    assert x.shape[0] == 3
