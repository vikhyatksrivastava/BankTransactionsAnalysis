from pyspark.sql import SparkSession


def spark_setup():
    spark = (SparkSession
             .builder
             .appName("BankTransaction")
             .config("spark.jars", "../../../../postgresql-42.7.3.jar")
             .getOrCreate())
    return spark


def jdbc_connection_setup():
    db_url = "jdbc:postgresql://localhost:5432/PersonalVikhyat"
    db_properties = {
        "user": "postgres",
        "password": "Pa55word@0",
        "driver": "org.postgresql.Driver",
        "schema": "BankData"
    }
    # print(spark.read.jdbc(url=db_url, table=table_name, properties=db_properties))
    # return spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)

    return db_url, db_properties
