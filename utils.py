from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "ProductCategoryMapping") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def read_csv(spark: SparkSession, path: str, **options) -> DataFrame:
    default_options = {
        "header": True,
        "inferSchema": True
    }
    default_options.update(options)
    return spark.read.csv(path, **default_options)