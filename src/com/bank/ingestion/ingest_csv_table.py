from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col

from com.bank.config.config import jdbc_connection_setup
from com.bank.transactions.ReadTransactions import read_from_table


def read_csv(file_name, spark: SparkSession):
    return spark.read.csv(file_name, header=True, inferSchema=True)


def add_col_to_df(df, new_col_name, col_value):
    return df.withColumn(new_col_name, lit(col_value))


def ingest_to_table(spark: SparkSession, file_name, target_table, bank_name):
    df = read_csv(file_name, spark)
    # df.show(5)
    if bank_name == "LLoydsCA":
        updated_df = df.withColumnRenamed("Transaction Date", "transaction_date").withColumnRenamed(
            "Transaction Type", "transaction_type").withColumnRenamed("Sort Code", "sort_code").withColumnRenamed(
            "Account Number", "account_number").withColumnRenamed("Transaction Description",
                                                                  "transaction_description").withColumnRenamed(
            "Debit Amount", "debit_amount").withColumnRenamed("Credit Amount", "credit_amount").withColumnRenamed(
            "Balance",
            "balance")
        df_with_new_col = add_col_to_df(updated_df, "bank_name", bank_name)
        db_url, db_properties = jdbc_connection_setup()
        df_with_new_col.write.jdbc(url=db_url, table=target_table, mode="append", properties=db_properties)
        read_from_table(spark, target_table)
    elif bank_name == "AmexCC":
        updated_df = df.withColumnRenamed("Date", "transaction_date").withColumnRenamed("Description",
                                                                                        "transaction_description")
        df_with_debit_credit = updated_df.withColumn("debit_amount",
                                                     when(col("Amount") >= 0, col("Amount")).otherwise(None)).withColumn(
            "credit_amount",
            when(col("Amount") < 0, col("Amount")).otherwise(None)).withColumn("transaction_type",
                                                                                lit(None).cast("string")).withColumn(
            "sort_code", lit(None).cast("string")).withColumn("account_number", lit(None).cast("integer")).withColumn(
            "balance", lit(None).cast("double")).drop("Amount")
        df_with_new_col = add_col_to_df(df_with_debit_credit, "bank_name", bank_name)
        db_url, db_properties = jdbc_connection_setup()
        df_with_new_col.write.jdbc(url=db_url, table=target_table, mode="append", properties=db_properties)
        read_from_table(spark, target_table)
    else:
        print("No Action taken")
