import argparse
from typing import Dict, Tuple, Type

import torch
import torch.nn.functional as F
from tqdm import tqdm

from tgrag.dataset.temporal_dataset import TemporalDataset
from tgrag.encoders.rni_encoding import RNIEncoder
from tgrag.gnn.GAT import GAT
from tgrag.gnn.gCon import GCN
from tgrag.gnn.SAGE import SAGE
from tgrag.utils.path import get_root_dir

MODEL_CLASSES: Dict[str, Type[torch.nn.Module]] = {
    'GCN': GCN,
    'GAT': GAT,
    'SAGE': SAGE,
}

ENCODER_CLASSES = {
    'RNI': RNIEncoder,
}


def train(
    model: torch.nn.Module,
    data: TemporalDataset,
    train_idx: torch.Tensor,
    optimizer: torch.optim.Adam,
) -> float:
    model.train()

    optimizer.zero_grad()
    out = model(data.x, data.adj_t)[train_idx]
    loss = F.mse_loss(out.squeeze(), data.y.squeeze(1)[train_idx])
    loss.backward()
    optimizer.step()

    return loss.item()


def test(
    model: torch.nn.Module, data: TemporalDataset, split_idx: Dict
) -> Tuple[float, float, float]:
    model.eval()
    out = model(data.x, data.adj_t)

    y_true = data.y
    y_pred = out

    train_mse = F.mse_loss(
        y_pred[split_idx['train']], y_true[split_idx['train']]
    ).item()
    valid_mse = F.mse_loss(
        y_pred[split_idx['valid']], y_true[split_idx['valid']]
    ).item()
    test_mse = F.mse_loss(y_pred[split_idx['test']], y_true[split_idx['test']]).item()

    return train_mse, valid_mse, test_mse


def main() -> None:
    parser = argparse.ArgumentParser(description='Static Graph (GNN) Experiment.')
    parser.add_argument('--device', type=int, default=0, help='Device to be used.')
    parser.add_argument(
        '--seed', type=int, default=42, help='Seed for reproducible results.'
    )
    parser.add_argument('--log_steps', type=int, default=1, help='Log steps.')
    parser.add_argument(
        '--model',
        type=str,
        choices=MODEL_CLASSES.keys(),
        default='GCN',
        help='The GNN class name.',
    )
    parser.add_argument(
        '--encoder',
        type=str,
        choices=ENCODER_CLASSES.keys(),
        default='RNI',
        help='Encoder name.',
    )
    parser.add_argument(
        '--encoder_col',
        type=str,
        default='random',
        help='The column for which the encoder will act on.',
    )
    parser.add_argument(
        '--num_layers',
        type=int,
        default=3,
        help='Number of layers or number of iterations in message passing.',
    )
    parser.add_argument(
        '--hidden_channels',
        type=int,
        default=256,
        help='Inner dimension of update weight matrix.',
    )
    parser.add_argument('--dropout', type=float, default=0.5, help='Dropout value.')
    parser.add_argument('--lr', type=float, default=0.01, help='Learning rate.')
    parser.add_argument('--epochs', type=int, default=500, help='Number of epochs.')
    parser.add_argument('--runs', type=int, default=10, help='Number of trials.')
    parser.add_argument('--help')
    args = parser.parse_args()
    print(args)

    device = f'cuda:{args.device}' if torch.cuda.is_available() else 'cpu'
    device = torch.device(device)

    root_dir = get_root_dir()

    model_class = MODEL_CLASSES[args.model]
    ENCODER_CLASSES[args.encoder]

    encoding_dict = {args.encoder_col: args.encoder}

    dataset = TemporalDataset(
        root=f'{root_dir}/data/crawl-data/temporal',
        encoding=encoding_dict,
    )
    data = dataset[0]
    data = data.to(device)

    split_idx = dataset.get_idx_split()
    train_idx = split_idx['train'].to(device)

    model = model_class(
        data.num_features, args.hidden_channels, 1, args.num_layers, args.dropout
    ).to(device)

    for run in tqdm(range(args.runs), desc='Runs'):
        model.reset_parameters()
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
        for epoch in tqdm(range(1, 1 + args.epochs), desc='Epochs'):
            loss = train(model, data, train_idx, optimizer)
            result = test(model, data, split_idx)

            if epoch % args.log_steps == 0:
                train_acc, valid_acc, test_acc = result
                print(
                    f'Run: {run + 1:02d}, '
                    f'Epoch: {epoch:02d}, '
                    f'Loss: {loss:.4f}, '
                    f'Train: {100 * train_acc:.2f}%, '
                    f'Valid: {100 * valid_acc:.2f}% '
                    f'Test: {100 * test_acc:.2f}%'
                )


if __name__ == '__main__':
    main()
