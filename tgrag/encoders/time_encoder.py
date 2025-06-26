import torch
from pandas import Series
from torch import Tensor


class TimeStampEncoder:
    def __init__(self, granularity: int):
        self.granularity = granularity

    def __call__(self, series: Series) -> Tensor:
        return torch.tensor(series.values)
