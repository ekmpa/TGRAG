from typing import Dict, Tuple

import pandas as pd
import torch
from torch import Tensor


def load_node_csv(
    path: str, index_col: int, encoders: Dict | None = None
) -> Tuple[Tensor | None, Dict]:
    df = pd.read_csv(path, index_col=index_col)
    mapping = {index: i for i, index in enumerate(df.index.unique())}

    x = None
    if encoders is not None:
        xs = []
        for key, encoder in encoders.items():
            if key in df.columns:
                xs.append(encoder[df[key]])
            else:
                # Global encoder (In our case the RNIEncoder)
                xs.append(encoder(len(df)))

        x = torch.cat(xs, dim=-1)

    return x, mapping


def load_edge_csv(
    path: str,
    src_index_col: str,
    dst_index_col: str,
    encoders: Dict | None = None,
) -> Tuple[Tensor, Tensor | None]:
    df = pd.read_csv(path)

    src = df[src_index_col]
    dst = df[dst_index_col]
    edge_index = torch.tensor([src, dst])

    edge_attr = None
    if encoders is not None:
        edge_attrs = []
        for key, encoder in encoders.items():
            if key in df.columns:
                edge_attrs.append(encoder(df[key]))
            else:
                # Global encoder (e.g RNIEncoder )
                edge_attrs.append(encoder(len(df)))

        edge_attr = torch.cat(edge_attrs, dim=-1)

    return edge_index, edge_attr
