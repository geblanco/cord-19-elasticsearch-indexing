from pyspark.sql import SparkSession


components_path = "/data/qaservers/search/spark/config/elasticsearch-hadoop-7.13.1"  # noqa: E501
elastispark = f"{components_path}/dist/elasticsearch-spark-20_2.11-7.13.1.jar"


def get_session(spark_config=None, es_config=None):
    spark_config = spark_config or {}
    es_config = es_config or {}
    sp_addr = spark_config.get("addr", "localhost")
    sp_port = spark_config.get("port", 7077)
    es_port = es_config.get("port", 9200)
    print(f"Connecting to spark {sp_addr}:{sp_port}")

    return SparkSession \
        .builder \
        .master(f"spark://{sp_addr}:{sp_port}") \
        .appName("ElasticSpark-1") \
        .config("spark.driver.extraClassPath", elastispark) \
        .config("spark.es.port", es_port) \
        .config("spark.driver.memory", "8G") \
        .config("spark.executor.memory", "12G") \
        .getOrCreate()
