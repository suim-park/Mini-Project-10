from library.lib import init_spark, read_csv, spark_sql_query, transform
from pyspark.sql import SparkSession, Row


def test_init_spark():
    spark = init_spark(app_name="PySpark Data Processing")
    assert isinstance(spark, SparkSession), "Test failed."
    print("Test initiation spark passed successfully.")


def test_read_csv():
    spark = init_spark(app_name="PySpark Data Processing")
    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)
    print(df)
    assert df.count() > 0, "Test failed."
    print("Test reading csv file passed successfully.")


def test_spark_sql_query():
    # 테스트를 위한 SparkSession 생성
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    csv_file_path = "penguins.csv"
    df = read_csv(spark, csv_file_path)
    result_df = spark_sql_query(spark, df)

    # 기대되는 결과 생성
    expected_data = [
        Row(species="Gentoo", max_bill_length=59.6, min_bill_length=40.9),
        Row(species="Adelie", max_bill_length=46.0, min_bill_length=32.1),
        Row(species="Chinstrap", max_bill_length=58.0, min_bill_length=40.9),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # 결과 DataFrame과 기대되는 DataFrame 비교
    assert result_df.collect() == expected_df.collect(), "Test failed."
    print("Test spark sql query passed successfully.")


def test_transform():
    # 테스트를 위한 SparkSession 생성
    spark = SparkSession.builder.appName("Add Bill Length Category Test").getOrCreate()

    # 예시로 사용할 데이터
    sample_data = [
        Row(bill_length_mm=35.0),
        Row(bill_length_mm=45.0),
        Row(bill_length_mm=55.0),
    ]
    df = spark.createDataFrame(sample_data)

    # add_bill_length_category 함수 호출
    result_df = transform(spark, df)

    # 결과 DataFrame에서 bill_length_category 컬럼의 값 검증
    categories = [row["bill_length_category"] for row in result_df.collect()]
    expected_categories = ["Short", "Medium", "Long"]

    assert categories == expected_categories, "Test failed!"

    print("Test transform passed successfully.")

    # 테스트 후 SparkSession 종료
    spark.stop()


if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_spark_sql_query()
    test_transform()
