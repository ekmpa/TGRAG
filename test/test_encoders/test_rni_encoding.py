import pytest
import torch

from tgrag.encoders.rni_encoding import RNIEncoder


def test_rni_call():
    rni = RNIEncoder()
    x = rni(5)
    assert isinstance(x, torch.Tensor)
    print(x)
