from pyspark.sql import SparkSession

from com.bank.config.config import jdbc_connection_setup


def read_from_table(spark: SparkSession, table_name):
    db_url, db_properties = jdbc_connection_setup()
    df = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)
    df.show(5)
