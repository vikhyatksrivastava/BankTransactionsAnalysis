from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from com.bank.config.config import jdbc_connection_setup
from com.bank.transactions.ReadTransactions import read_from_table


def read_csv(file_name, spark: SparkSession):
    return spark.read.csv(file_name, header=True, inferSchema=True)


def add_col_to_df(df, new_col_name, col_value):
    return df.withColumn(new_col_name, lit(col_value))


def ingest_to_table(spark: SparkSession, file_name, target_table, bank_name):
    df = read_csv(file_name, spark)
    updated_df = df.withColumnRenamed("Transaction Date", "transaction_date").withColumnRenamed(
        "Transaction Type", "transaction_type").withColumnRenamed("Sort Code", "sort_code").withColumnRenamed(
        "Account Number", "account_number").withColumnRenamed("Transaction Description",
                                                              "transaction_description").withColumnRenamed(
        "Debit Amount", "debit_amount").withColumnRenamed("Credit Amount", "credit_amount").withColumnRenamed("Balance",
                                                                                                              "balance")
    df_with_new_col = add_col_to_df(updated_df, "bank_name", bank_name)
    db_url, db_properties = jdbc_connection_setup()
    df_with_new_col.write.jdbc(url=db_url, table=target_table, mode="overwrite", properties=db_properties)
    read_from_table(spark, target_table)
