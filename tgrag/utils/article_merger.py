import gzip
import os
from typing import Dict, Tuple

import pandas as pd
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from tgrag.utils.matching import extract_registered_domain
from tgrag.utils.merger import Merger
from tgrag.utils.path import get_root_dir, get_wet_file_path


class ArticleMerger(Merger):
    """Merges a slice's WET files with the existing WAT-based graph.
    i.e, merge article-level to domain-level data for a given slice.
    """

    def __init__(self, output_dir: str, slice: str) -> None:
        super().__init__(output_dir)
        self.slice = slice
        self.matched_articles = 0
        self.unmatched_articles = 0
        self.article_nodes: Dict[
            str, Tuple[int, str, str]
        ] = {}  # url -> node_id, date, text
        self.next_node_id = (
            max((nid for nid, _ in self.domain_to_node.values()), default=0) + 1
        )

    def merge(self) -> None:
        wet_path = get_wet_file_path(self.slice, str(get_root_dir()))

        with gzip.open(wet_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type != 'conversion':
                    continue

                wet_content = self._extract_wet_content(record)

                if not wet_content['url'] or not wet_content['text']:
                    continue

                domain = self._normalize_domain(
                    extract_registered_domain(wet_content['url'])
                )

                if domain not in self.domain_to_node:
                    self.unmatched_articles += 1
                    continue

                self.matched_articles += 1
                domain_node_id, time_id = self.domain_to_node[domain]

                if wet_content['url'] not in self.article_nodes:
                    article_node_id = self.next_node_id
                    self.next_node_id += 1
                    self.article_nodes[wet_content['url']] = (
                        article_node_id,
                        wet_content['warc_date'],
                        wet_content['text'],
                    )
                else:
                    (article_node_id, _, _) = self.article_nodes[wet_content['url']]

                # Add 'contains' edge from domain to article
                self.edges.append(
                    (domain_node_id, article_node_id, time_id, 'contains')
                )

            total_articles = self.matched_articles + self.unmatched_articles
            match_pct = (
                (self.matched_articles / total_articles * 100)
                if total_articles > 0
                else 0
            )
            print(
                f'Matched {self.matched_articles} out of {total_articles} articles ({match_pct:.2f}%)'
            )

    def save(self) -> None:
        """Override save to handle both domain and article nodes."""
        os.makedirs(self.output_dir, exist_ok=True)

        # Ensure all edges have type & save edges
        processed_edges = [
            edge if len(edge) == 4 else (*edge, 'hyperlinks') for edge in self.edges
        ]
        pd.DataFrame(
            processed_edges, columns=['src', 'dst', 'time_id', 'edge_type']
        ).to_csv(os.path.join(self.output_dir, 'temporal_edges.csv'), index=False)

        # Domain nodes
        domain_node_rows = [
            {'domain': domain, 'node_id': node_id, 'time_id': time_id}
            for domain, (node_id, time_id) in self.domain_to_node.items()
        ]

        # Article nodes
        article_node_rows = [
            {'domain': url, 'node_id': node_id, 'date': date, 'text': text}
            for url, (node_id, date, text) in self.article_nodes.items()
        ]

        df_nodes = pd.DataFrame(domain_node_rows + article_node_rows)
        df_nodes.to_csv(
            os.path.join(self.output_dir, 'temporal_nodes.csv'), index=False
        )

    def _extract_wet_content(self, record: ArcWarcRecord) -> dict:
        headers = record.rec_headers
        url = headers.get_header('WARC-Target-URI')
        warc_date = headers.get_header('WARC-Date')
        record_id = headers.get_header('WARC-Record-ID')
        content_type = headers.get_header('Content-Type')
        text = record.content_stream().read().decode('utf-8', errors='ignore')

        return {
            'url': url,
            'warc_date': warc_date,
            'record_id': record_id,
            'content_type': content_type,
            'text': text,
        }


# def extract_core_name(hostname: str) -> str:
#     parts = hostname.lower().split('.')
#     if len(parts) < 2:
#         return hostname
#     # Heuristic: take the second-to-last part
#     return parts[-2]

# def domain_matches(url: str, domains_set: List[str]) -> bool:
#     try:
#         hostname = urlparse(url).hostname
#         if not hostname:
#             return False
#         url_core = extract_core_name(hostname)
#         return url_core in domains_set
#     except Exception:
#         return False


# def extract_domains_from_vertex_files(vertex_dir: str) -> Set[str]:
#     domains = set()
#     for path in glob.glob(os.path.join(vertex_dir, "part-*")):
#         with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
#             for line in f:
#                 line = line.strip()
#                 if line and not line.startswith("#"):
#                     domain = line.split()[0]
#                     domains.add(domain)
#     return domains


# def extract_wet_documents(
#     wet_file_path: str,
#     output_path: str,
#     max_docs: Optional[int] = None,
#     domain_filter_path: Optional[str] = None,
#     vertex_dir: Optional[str] = None,
# ) -> None:
#     assert os.path.isfile(wet_file_path), f'WET file not found: {wet_file_path}'
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)

#     domains_set = set()
#     if domain_filter_path:
#         with open(domain_filter_path, 'r') as f:
#             domains_set = {line.strip() for line in f if line.strip()}
#     elif vertex_dir:
#         print(f'Extracting domains from vertex files in: {vertex_dir}')
#         domains_set = extract_domains_from_vertex_files(vertex_dir)
#         print(f'Loaded {len(domains_set)} domains from vertices')

#     extracted = 0
#     with (
#         gzip.open(wet_file_path, 'rb') as stream,
#         open(output_path, 'w', encoding='utf-8') as out,
#     ):
#         for record in ArchiveIterator(stream):
#             if record.rec_type != 'conversion':
#                 continue

#             url = record.rec_headers.get_header('WARC-Target-URI')
#             if domains_set and not domain_matches(url, list(domains_set)):
#                 continue

#             doc = {
#                 'url': url,
#                 'timestamp': record.rec_headers.get_header('WARC-Date'),
#                 'warc_id': record.rec_headers.get_header('WARC-Record-ID'),
#                 'content': record.content_stream()
#                 .read()
#                 .decode('utf-8', errors='ignore'),
#             }

#             out.write(json.dumps(doc) + '\n')
#             extracted += 1

#             if max_docs and extracted >= max_docs:
#                 break

#     print(f'Extracted {extracted} docs to {output_path}')


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--wet_file", required=True)
#     parser.add_argument("--output_jsonl", required=True)
#     parser.add_argument("--filter_domains", help="Path to file with 1 domain per line")
#     parser.add_argument("--vertex_dir", help="If no domain list is given, extract from graph vertices")
#     parser.add_argument("--max_docs", type=int, default=None)
#     args = parser.parse_args()

#     extract_wet_documents(
#         wet_file_path=args.wet_file,
#         output_path=args.output_jsonl,
#         max_docs=args.max_docs,
#         domain_filter_path=args.filter_domains,
#         vertex_dir=args.vertex_dir
#     )
