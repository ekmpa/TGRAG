import glob
import os
import shutil
import subprocess


def run_spark_wat_extraction(
    input_file: str,
    output_table: str,
    script_path: str = "external/cc-pyspark/wat_extract_links.py",
):
    """
    Run the WAT link extraction script using Spark via subprocess.

    Parameters:
        input_file (str): Path to the input file listing WAT files.
        output_table (str): Output location/table name.
        script_path (str): Path to the Spark Python script.
    """
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        raise EnvironmentError("SPARK_HOME is not set in environment variables.")

    spark_submit = os.path.join(spark_home, "bin", "spark-submit")

    try:
        result = subprocess.run(
            [spark_submit, script_path, input_file, output_table],
            check=True,
            capture_output=True,
            text=True,
        )
        print("Spark job completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error during wat_extract_links job:")
        print(e.stderr)
        raise


def run_hostlinks_to_graph(
    input_table: str,
    output_table: str,
    text_output_path: str,
    script_path: str = "external/cc-pyspark/hostlinks_to_graph.py",
    output_format: str = "parquet",
    output_compression: str = "gzip",
    log_level: str = "WARN",
):
    """
    Run the hostlinks_to_graph Spark job via subprocess.

    Parameters:
        input_table (str): Path to the input Spark table (e.g., from WAT output).
        output_table (str): Output path/table for the webgraph.
        text_output_path (str): Optional path to save graph as plain text.
        script_path (str): Path to the hostlinks_to_graph.py script inside external/cc-pyspark.
        output_format (str): Output format (default: 'parquet').
        output_compression (str): Compression format (default: 'gzip').
        log_level (str): Spark log level (default: 'WARN').
    """
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        raise EnvironmentError("SPARK_HOME is not set in environment variables.")

    spark_submit = os.path.join(spark_home, "bin", "spark-submit")

    cmd = [
        spark_submit,
        script_path,
        input_table,
        output_table,
        "--output_format",
        output_format,
        "--output_compression",
        output_compression,
        "--log_level",
        log_level,
        "--save_as_text",
        text_output_path,
    ]

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Spark job (hostlinks_to_graph) completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error during hostlinks_to_graph Spark job:")
        print(e.stderr)
        raise


def move_and_rename_webgraph_outputs(
    source_base: str = "spark-warehouse/webgraph_text",
    target_base: str = "external/cc-webgraph",
):
    """
    Renames and moves webgraph text output files to the cc-webgraph folder.

    Parameters:
        source_base (str): Base path to the source output folders (default: 'spark-warehouse/webgraph_text').
        target_base (str): Path to the target directory (default: 'external/cc-webgraph').
    """
    os.makedirs(target_base, exist_ok=True)

    for subdir in ["edges", "vertices"]:
        source_dir = os.path.join(source_base, subdir)
        target_filename = f"{subdir}.txt.gz"

        # Find the .txt.gz file (assumes only one file in each subdir)
        matches = glob.glob(os.path.join(source_dir, "*.txt.gz"))
        if not matches:
            raise FileNotFoundError(f"No .txt.gz file found in {source_dir}")

        source_file = matches[0]
        target_path = os.path.join(target_base, target_filename)

        # Move and rename
        shutil.move(source_file, target_path)
        print(f"Moved {source_file} → {target_path}")


def run_webgraph_ranking(
    graph_name: str,
    vertices_file: str = "vertices.txt.gz",
    edges_file: str = "edges.txt.gz",
    output_dir: str = "output",
    working_dir: str = "external/cc-webgraph",
):
    """
    Run the webgraph_ranking process_webgraph.sh script using subprocess.

    Parameters:
        graph_name (str): The name for the graph processing job.
        vertices_file (str): The name of the vertices file (default: 'vertices.txt.gz').
        edges_file (str): The name of the edges file (default: 'edges.txt.gz').
        output_dir (str): Directory where output will be stored (relative to working_dir).
        working_dir (str): Path to the external/cc-webgraph repo.
    """
    script_path = "./src/script/webgraph_ranking/process_webgraph.sh"
    cmd = [script_path, graph_name, vertices_file, edges_file, output_dir]

    try:
        result = subprocess.run(
            cmd, cwd=working_dir, check=True, capture_output=True, text=True
        )
        print("WebGraph ranking completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error during webgraph ranking:")
        print(e.stderr)
        raise


def main() -> None:
    run_spark_wat_extraction(
        input_file="input_paths.txt", output_table="wat_output_table"
    )

    run_hostlinks_to_graph(
        input_table="spark-warehouse/wat_output_table",
        output_table="webgraph_output_table",
        text_output_path="spark-warehouse/webgraph_text",
    )

    move_and_rename_webgraph_outputs()

    run_webgraph_ranking(graph_name="my_graph", output_dir="rank_output")
