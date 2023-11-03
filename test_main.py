import main
from pyspark.sql import SparkSession

def test_init_spark():
    spark = main.init_spark()
    assert isinstance(spark, SparkSession)

def test_read_csv():
    spark = main.init_spark()
    df = main.read_csv(spark)
    assert df.count() > 0

def test_spark_sql_query():
    # 테스트를 위한 SparkSession 생성
    spark = SparkSession.builder \
        .appName("Spark SQL Query Test") \
        .getOrCreate()

    # 테스트 데이터 프레임 생성
    test_data = [
        Row(species="Adelie", bill_length_mm=34.4),
        Row(species="Adelie", bill_length_mm=46.0),
        Row(species="Gentoo", bill_length_mm=50.4)
    ]
    df = spark.createDataFrame(test_data)

    # spark_sql_query 함수 호출
    spark_sql_query(spark, df)

    # 테스트 후 SparkSession 종료
    spark.stop()

def test_add_bill_length_category():
    # 테스트를 위한 SparkSession 생성
    spark = SparkSession.builder \
        .appName("Add Bill Length Category Test") \
        .getOrCreate()

    # 테스트 데이터 프레임 생성
    test_data = [
        Row(bill_length_mm=34.4),
        Row(bill_length_mm=46.0),
        Row(bill_length_mm=50.4)
    ]
    df = spark.createDataFrame(test_data)

    # add_bill_length_category 함수 호출
    result_df = add_bill_length_category(df)
    result_df.show()

    # 추가적으로 결과를 검증할 수 있는 코드를 여기에 넣을 수 있습니다.
    # 예: 첫 번째 로우의 bill_length_category 값이 "Short"인지 확인
    first_row = result_df.first()
    assert first_row['bill_length_category'] == "Short", "Test failed!"

    # 테스트 후 SparkSession 종료
    spark.stop()

if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_spark_sql_query()
    test_add_bill_length_category()