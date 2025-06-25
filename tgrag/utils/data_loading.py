from typing import Dict, Tuple

import pandas as pd
import torch
from torch import Tensor


def load_node_csv(
    path: str, index_col: int, encoders=None, **kwargs
) -> Tuple[Tensor | None, Dict]:
    df = pd.read_csv(path, index_col=index_col, **kwargs)
    mapping = {index: i for i, index in enumerate(df.index.unique())}

    x = None
    if encoders is not None:
        xs = [encoder(df[col]) for col, encoder in encoders.items()]
        x = torch.cat(xs, dim=-1)

    return x, mapping


def load_edge_csv(
    path: str,
    src_index_col: str,
    dst_index_col: str,
    encoders=None,
    **kwargs,
) -> Tuple[Tensor, Tensor | None]:
    df = pd.read_csv(path, **kwargs)

    src = df[src_index_col]
    dst = df[dst_index_col]
    edge_index = torch.tensor([src, dst])

    edge_attr = None
    if encoders is not None:
        edge_attrs = [encoder(df[col]) for col, encoder in encoders.items()]
        edge_attr = torch.cat(edge_attrs, dim=-1)

    return edge_index, edge_attr
