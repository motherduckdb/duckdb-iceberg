from scripts.data_generators.integration_config import get_spark_runtime


def iceberg_runtime_configuration(runtime=None):
    if runtime is None:
        raise ValueError("A Spark runtime must be provided explicitly")
    resolved_runtime = get_spark_runtime(runtime)
    return {
        "spark_version": str(resolved_runtime.spark_version),
        "scala_binary_version": resolved_runtime.scala_binary_version,
        "iceberg_library_version": resolved_runtime.iceberg_library_version,
    }
