"""Domain matching logic,
used for label matching, and WET-WAT matching.
"""

import gzip
import re
from urllib.parse import urlparse

import pandas as pd
import tldextract


def extract_from_line(line: str) -> str:
    tokens = line.lower().split('.')
    if len(tokens) >= 2:
        domain = tokens[1] + '.' + tokens[0]
    else:
        # this case shouldn't happen (fallback)
        domain = tokens[0]
    return domain


def extract_domain_from_url(url: str) -> str:
    try:
        return urlparse(url).hostname or ''
    except Exception:
        return ''


def extract_registered_domain(url: str) -> str:
    ext = tldextract.extract(url)
    return f'{ext.domain}.{ext.suffix}' if ext.domain and ext.suffix else ''


def extract_graph_domains(filepath: str) -> pd.DataFrame:
    parsed = []
    with gzip.open(filepath, 'rt', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            line = line.strip()
            line = re.sub(r'^\s*\d+\s+', '', line)  # remove node id

            if not line:
                continue

            domain = extract_from_line(line)

            parsed.append((i, domain))

    return pd.DataFrame(parsed, columns=['node_id', 'match_domain'])
