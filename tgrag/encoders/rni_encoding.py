import torch
from torch import Tensor


class RNIEncoder:
    def __init__(self, seed: int | None = None):
        self.seed = seed

    def __call__(self, length: int) -> Tensor:
        if self.seed is not None:
            torch.manual_seed(self.seed)
        return torch.randn(length)
