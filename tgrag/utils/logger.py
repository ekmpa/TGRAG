import logging
from typing import Any, List, Optional, Tuple

import torch


def setup_basic_logging(
    log_file_path: Optional[str] = None,
    log_file_logging_level: int = logging.DEBUG,
    stream_logging_level: int = logging.INFO,
) -> None:
    handlers: List[logging.Handler] = []

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_logging_level)
    stream_handler.setFormatter(
        logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    )
    handlers.append(stream_handler)

    if log_file_path is not None:
        file_handler = logging.FileHandler(filename=log_file_path, mode='w')
        file_handler.setLevel(log_file_logging_level)
        file_handler.setFormatter(
            logging.Formatter(
                '[%(asctime)s] %(levelname)s [%(processName)s %(threadName)s %(name)s.%(funcName)s:%(lineno)d] %(message)s',
            )
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(levelname)s [%(processName)s %(threadName)s %(name)s.%(funcName)s:%(lineno)d] %(message)s',
        handlers=handlers,
    )


class Logger(object):
    def __init__(self, runs: int, info: Any | None = None):
        self.info = info
        self.results: List[Any] = [[] for _ in range(runs)]

    def add_result(self, run: int, result: Tuple[float, float, float]) -> None:
        assert len(result) == 3
        assert run >= 0 and run < len(self.results)
        self.results[run].append(result)

    def print_statistics(self, run: int | None = None) -> None:
        if run is not None:
            result = 100 * torch.tensor(self.results[run])
            argmin = result[:, 1].argmin().item()
            print(f'Run {run + 1:02d}:')
            print(f'Lowest Train Loss: {result[:, 0].min():.2f}')
            print(f'Lowest Valid Loss: {result[:, 1].min():.2f}')
            print(f'  Final Train Loss: {result[argmin, 0]:.2f}')
            print(f'   Final Test Loss: {result[argmin, 2]:.2f}')
        else:
            result = 100 * torch.tensor(self.results)

            best_results = []
            for r in result:
                train1 = r[:, 0].min().item()
                valid = r[:, 1].min().item()
                train2 = r[r[:, 1].argmin(), 0].item()
                test = r[r[:, 1].argmin(), 2].item()
                best_results.append((train1, valid, train2, test))

            best_result = torch.tensor(best_results)

            print('All runs:')
            r = best_result[:, 0]
            print(f'Lowest Train Loss: {r.mean():.2f} ± {r.std():.2f}')
            r = best_result[:, 1]
            print(f'Lowest Valid Loss: {r.mean():.2f} ± {r.std():.2f}')
            r = best_result[:, 2]
            print(f'  Final Train Loss: {r.mean():.2f} ± {r.std():.2f}')
            r = best_result[:, 3]
            print(f'   Final Test Loss: {r.mean():.2f} ± {r.std():.2f}')
