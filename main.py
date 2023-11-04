from library.lib import init_spark, read_csv, spark_sql_query, transform


if __name__ == "__main__":
    spark = init_spark(app_name="PySpark Data Processing")

    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)

    # 원본 데이터 출력
    print("Original Data:")
    df.show()

    # Spark SQL 쿼리를 사용한 데이터 출력
    print("Data After Spark SQL Query:")
    spark_sql_query(spark, df)

    # bill_length_category 추가한 데이터 출력
    df_with_category = transform(spark, df)
    print("Data After Adding Bill Length Category:")
    df_with_category.show()
