# TGRAG
Data analysis for TG/RAG project @ CDL 

## Getting Started

### Prerequisites
The project uses [uv](https://docs.astral.sh/uv/) to manage and lock project dependencies for a consistent and reproducible environment. If you do not have `uv` installed on your system, visit [this page](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

**Note**: to get `uv`, run: 

```sh
pip install uv
# or 
brew install uv
```

### Installation

```sh
# Clone the repo
git clone git@github.com:ekmpa/TGRAG.git

# Enter the repo directory
cd TGRAG

# Install core dependencies into an isolated environment
uv sync 

# The isolated env is .venv
source .venv/bin/activate

# If you have your environment and want to sync that instead, run 
uv sync --active
```
## Usage

### Running full data analytics
```sh

```


## Running Interactions

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

## CC-data loading 

Clone the two external repos and move them to `external/`: 

```
git clone https://github.com/commoncrawl/cc-webgraph
git clone https://github.com/commoncrawl/cc-pyspark
mv cc-webgraph cc-pyspark external
```

They use java (maven) and pyspark. 
- For Java, need to `cd external/cc-webgraph`, then `mvn package`.
- For Apache Spark, need to `brew install apache-spark`.
- For both, need to set the global variables, `JAVA_HOME` and `SPARK_HOME`, to the proper path. 

To get the WAT files, use the loading pipeline in `scripts/load_pipeline.sh`. 

- To get available time slices, `curl -s https://index.commoncrawl.org/collinfo.json | jq '.[].id'`

---
### cc-webspark: 

The external repo cc-webspark will create the following analytics for the graph: 

```graph_name.graph, graph_name.offsets, .indegrees, .outdegrees```: actual WebGraph binary files.

```graph_name-ranks.txt.gz```: list of node ranks (PageRank).

```.scc, .wcc```: strongly/weakly connected components.

```.stats, .properties```: metadata about the graph.

```.txt.gz distribution files``` (e.g., indegree-distrib, outdegree-distrib): useful for plotting degree distributions

---

<!-- ### Running external repos on MacOS

They are designed for Linux and need a few adjustments to run on MacOS: 

1. In `external/cc-webgraph/src/script/webgraph_ranking/webgraph_config.sh`, since `free` is not a command on MacOS, 

    ```sh 
    # Replace this line:
    MEM_20PERC=$(free -g | perl -ne 'do { print 1+int($1*.2), "g"; last } if /(\d+)/') 

    # With this: 
    TOTAL_MEM_BYTES=$(sysctl -n hw.memsize)
    SORT_BUFFER_SIZE=$(awk -v mem="$TOTAL_MEM_BYTES" '
    BEGIN {
        gb = int(mem / 1073741824)
        sort_buf = int(gb * 0.2)
        if (sort_buf < 1) sort_buf = 1
        print sort_buf "g"
    }')
    ```

2. In `external/cc-webgraph/src/script/webgraph_ranking/process_webgraph.sh`, replace the two `zcat` commands at lines 234 and 237 by `gunzip -c`

Then, can use the `run_external` file normally.  -->


