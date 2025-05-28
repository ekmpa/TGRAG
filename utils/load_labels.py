import pandas as pd
import gzip
import tldextract

def load_credibility_scores(path: str, use_core: bool = False) -> pd.DataFrame:
    """Load and normalize credibility scores, returning either core or registered domain."""
    cred_df = pd.read_csv(path)
    cred_df["domain"] = (
        cred_df["domain"]
        .str.strip()
        .str.lower()
        .str.replace(r"^www\.", "", regex=True)
    )

    if use_core:
        cred_df["match_domain"] = cred_df["domain"].apply(
            lambda d: tldextract.extract(d).domain
        )
    else:
        cred_df["match_domain"] = cred_df["domain"].apply(
            lambda d: f"{tldextract.extract(d).domain}.{tldextract.extract(d).suffix}"
        )

    return cred_df[["match_domain", "pc1"]]


def extract_graph_domains(filepath: str, use_core: bool = False) -> pd.DataFrame:
    """Extract domains from WebGraph vertices (core or registered)."""
    parsed = []

    with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            line = line.strip().lower()
            if not line:
                continue

            domain_part = line.split("\t", 1)[-1]
            ext = tldextract.extract(domain_part)

            if use_core and ext.domain:
                parsed.append((i, ext.domain))
            elif ext.domain and ext.suffix:
                parsed.append((i, f"{ext.domain}.{ext.suffix}"))

    return pd.DataFrame(parsed, columns=["node_id", "match_domain"])


def main():
    use_core = True  # Set to True for core domain matching, False for registered domains

    cred_df = load_credibility_scores("domain-quality-ratings/data/domain_pc1.csv", use_core=use_core)
    print("INFO: Loaded credibility scores")
    #print(cred_df.head())

    vertices_df = extract_graph_domains("external/cc-webgraph/vertices.txt.gz", use_core=use_core)
    #print("\nSample parsed domains:")
    #print(vertices_df.sample(10))

    enriched_df = pd.merge(vertices_df, cred_df, on="match_domain", how="inner")
    enriched_df.to_csv("external/cc-webgraph/node_credibility.csv", index=False)

    print("INFO: Merge done.")
    #print(enriched_df.head())
    annotated = len(enriched_df)
    total = len(vertices_df)
    percentage = (annotated / total) * 100
    print(f"{annotated} / {total} nodes annotated with credibility scores ({percentage:.2f}%).")

if __name__ == "__main__":
    main()