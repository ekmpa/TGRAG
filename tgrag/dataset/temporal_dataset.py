import os
from typing import Dict, List

import pandas as pd
import torch
from torch_geometric.data import Data, InMemoryDataset
from torch_geometric.utils import to_torch_csr_tensor

from tgrag.utils.data_loading import load_edge_csv, load_node_csv


class TemporalDataset(InMemoryDataset):
    def __init__(self, root, encoding=None, transform=None, pre_transform=None) -> None:
        self.encoding = encoding
        super().__init__(root, transform, pre_transform)
        self.data, self.slices = torch.load(self.processed_paths[0], weights_only=False)

    @property
    def raw_dir(self) -> str:
        return os.path.join(self.root)

    @property
    def raw_file_names(self) -> List[str]:
        return ['temporal_nodes.csv', 'temporal_edges.csv']

    @property
    def processed_file_names(self) -> List[str]:
        return ['data.pt']

    def download(self) -> None:
        pass

    def process(self) -> None:
        node_path = os.path.join(self.raw_dir, 'temporal_nodes.csv')
        edge_path = os.path.join(self.raw_dir, 'temporal_edges.csv')
        x_full, mapping = load_node_csv(
            path=node_path,
            index_col=1,  # 'node_id'
            encoders=self.encoding,
        )

        if x_full is None:
            raise TypeError('X is None type. Please use an encoding.')

        df = pd.read_csv(node_path)
        df = df.set_index('node_id').loc[mapping.keys()]
        cr_score = torch.tensor(df['cr_score'].values, dtype=torch.float).unsqueeze(1)

        edge_index, edge_attr = load_edge_csv(
            path=edge_path, src_index_col='src', dst_index_col='dst', encoders=None
        )

        adj_t = to_torch_csr_tensor(edge_index, size=(x_full.size(0), x_full.size(0)))

        data = Data(x=x_full, y=cr_score, edge_index=edge_index, edge_attr=edge_attr)
        data.adj_t = adj_t

        torch.save(self.collate([data]), self.processed_paths[0])

    def get_idx_split(self) -> Dict:
        n = self[0].num_nodes
        return {
            'train': torch.arange(0, int(0.6 * n)),
            'valid': torch.arange(int(0.6 * n), int(0.8 * n)),
            'test': torch.arange(int(0.8 * n), n),
        }
