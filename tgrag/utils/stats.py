"""Experiments and statistical evaluation."""

import os

from path import get_root_dir


def experiment(domain: str) -> None:
    """Given a domain e.g apple.com, performs experiments
    on its subnetwork.
    """
    print(f'experiment on {domain}')


def experiments(path: str) -> None:
    """Given the path to /sub-networks, goes through each
    to perform experiments.
    """
    for entry in os.listdir(path):
        if entry.startswith('sub_vertices_domain_'):
            parts = entry.split('_')
            if len(parts) >= 2:
                domain = '.'.join(
                    parts[-2:]
                )  # get the last two parts and join with '.'
                experiment(domain)


def main() -> None:
    experiments(f'{get_root_dir()}/data/crawl-data/sub-networks')


if __name__ == '__main__':
    main()
