import torch
from pandas import Series
from torch import Tensor

from tgrag.encoders.encoder import Encoder


class TimeStampEncoder(Encoder):
    def __init__(self, granularity: int):
        self.granularity = granularity

    def __call__(self, series: Series) -> Tensor:
        return torch.tensor(series.values)
