import torch
from pandas import DataFrame
from torch import Tensor


class RNIEncoder:
    def __init__(self, seed: int | None = None):
        self.seed = seed

    def __call__(self, df: DataFrame) -> Tensor:
        if self.seed is not None:
            torch.manual_seed(self.seed)
        num_nodes = len(df)
        return torch.randn(num_nodes)
