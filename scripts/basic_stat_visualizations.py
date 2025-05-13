import gzip

import matplotlib.pyplot as plt
import pandas as pd


def page_rank_distribution():

    # Load the ranks file into a DataFrame
    with gzip.open("data/output_dir/graph_name-ranks.txt.gz", "rt") as f:
        df = pd.read_csv(
            f,
            sep="\t",
            comment="#",
            header=None,
            names=["harmonicc_pos", "harmonicc_val", "pr_pos", "pr_val", "host_rev"],
        )

    # Plot PageRank score distribution
    plt.figure(figsize=(10, 6))
    plt.hist(df["pr_val"], bins=100, log=True)
    plt.title("PageRank Score Distribution")
    plt.xlabel("PageRank Score")
    plt.ylabel("Frequency (log scale)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def out_degree_distribution():
    # Load outdegree distribution
    with gzip.open("data/output_dir/graph_name-outdegree-distrib.txt.gz", "rt") as f:
        df_out = pd.read_csv(
            f, sep="\t", comment="#", header=None, names=["outdegree", "count"]
        )

    # Plot distribution
    plt.figure(figsize=(10, 6))
    plt.bar(df_out["outdegree"], df_out["count"], width=1)
    plt.yscale("log")  # Log scale for better visibility
    plt.title("Outdegree Distribution")
    plt.xlabel("Outdegree")
    plt.ylabel("Number of Nodes (log scale)")
    plt.grid(True, which="both", axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()


def in_degree_distribution():
    # Load indegree distribution
    with gzip.open("data/output_dir/graph_name-indegree-distrib.txt.gz", "rt") as f:
        df_in = pd.read_csv(
            f, sep="\t", comment="#", header=None, names=["indegree", "count"]
        )

    # Plot distribution
    plt.figure(figsize=(10, 6))
    plt.bar(df_in["indegree"], df_in["count"], width=1)
    plt.yscale("log")  # Log scale for better visibility
    plt.title("Indegree Distribution")
    plt.xlabel("Indegree")
    plt.ylabel("Number of Nodes (log scale)")
    plt.grid(True, which="both", axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.show()


def main() -> None:
    page_rank_distribution()
    out_degree_distribution()
    in_degree_distribution()


if __name__ == "__main__":
    main()
