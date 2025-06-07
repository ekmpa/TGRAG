import gzip

import pandas as pd


def create_csv_edges_vertices() -> None:
    for element in {'edges', 'vertices'}:
        with gzip.open(f'data/{element}.txt.gz', 'rt') as f:
            hostnames = [line.strip() for line in f]

        df_nodes = pd.DataFrame({'id': range(len(hostnames)), 'label': hostnames})

        df_nodes.to_csv(f'data/{element}_nodes.csv', index=False)


def main() -> None:
    print('Creating csv files.')
    create_csv_edges_vertices()


if __name__ == '__main__':
    main()
