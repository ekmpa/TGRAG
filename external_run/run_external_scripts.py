import os
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


def main() -> None:
    run_spark_wat_extraction(
        input_file="input_paths.txt", output_table="wat_output_table"
    )

    run_hostlinks_to_graph(
        input_table="spark-warehouse/wat_output_table",
        output_table="webgraph_output_table",
        text_output_path="spark-warehouse/webgraph_text",
    )
