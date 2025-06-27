from abc import ABC, abstractmethod
from typing import Any


class Encoder(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        pass
