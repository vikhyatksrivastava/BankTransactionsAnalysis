from com.bank.config.config import spark_setup
from com.bank.ingestion.ingest_csv_table import ingest_to_table


def main():
    spark = spark_setup()
    file_name = "../../../../../../postgre/lloyds_current_account.csv"
    target_table = "\"BankData\".transaction"
    bank_code = 1
    if bank_code == 1:
        ingest_to_table(spark, file_name, target_table, "LLoydsCA")
    elif bank_code == 2:
        ingest_to_table(spark, file_name, target_table, "LLoydsCC")
    elif bank_code == 3:
        ingest_to_table(spark, file_name, target_table, "HalCA")
    elif bank_code == 4:
        ingest_to_table(spark, file_name, target_table, "HalCC")
    elif bank_code == 5:
        ingest_to_table(spark, file_name, target_table, "AmexCC")
    else:
        print("Incorrect Option")


if __name__ == '__main__':
    main()
