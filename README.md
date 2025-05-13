# TGRAG
Data analysis for TG/RAG project @ CDL 


## Getting Started

### Prerequisites
The project uses [uv](https://docs.astral.sh/uv/) to manage and lock project dependencies for a consistent and reproducible environment. If you do not have `uv` installed on your system, visit [this page](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

**Note**: If you have `pip` you can just invoke:

```sh
pip install uv
```

### Installation

```sh
# Clone the repo
git clone git@github.com:ekmpa/TGRAG.git

# Enter the repo directory
cd TGRAG

# Install core dependencies into an isolated environment
uv sync
```

# Running Interactions

```sh
uv add --dev ipykernel
```

```sh
uv run ipython kernel install --user --env VIRTUAL_ENV $(pwd)/.venv --name=TGRAG
```

```sh
uv run --with jupyter jupyter lab
```

Now you can run the ```interaction.ipynb``` notebook through Jupyter Lab.

## cc-webspark: 

The external repo cc-webspark will create the following analytics for the graph: 

```graph_name.graph, graph_name.offsets, .indegrees, .outdegrees```: actual WebGraph binary files.

```graph_name-ranks.txt.gz```: list of node ranks (PageRank).

```.scc, .wcc```: strongly/weakly connected components.

```.stats, .properties```: metadata about the graph.

```.txt.gz distribution files``` (e.g., indegree-distrib, outdegree-distrib): useful for plotting degree distributions
