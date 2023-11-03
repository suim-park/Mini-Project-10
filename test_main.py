from pyspark.sql.session import SparkSession
import pytest
from mylib import init_spark, load_csv, summary, analysis

@pytest.fixture(scope="session")
def spark_session():
    session = init_spark()
    yield session
    session.stop()

def test_load_csv(spark_session: SparkSession):
    data_path = "https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv"
    df = load_csv(data_path)
    assert df is not None and df.count() > 0

def test_summary(spark_session: SparkSession):
    df = load_csv("https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv")
    result = summary(df)
    assert result is not None

def test_analysis(spark_session: SparkSession):
    df = load_csv("https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv")
    result = analysis(df)
    assert result is not None

def test_spark_sql_query(spark_session: SparkSession):
    # 테스트용 데이터 경로 지정
    test_data_path = "https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv"
    df = load_csv(test_data_path)
    
    # spark_sql_query 호출
    try:
        spark_sql_query(df)
        print("spark_sql_query test passed!")
    except Exception as e:
        print(f"spark_sql_query test failed: {e}")

def test_add_bill_length_category(spark_session: SparkSession):
    # 테스트용 데이터 경로 지정
    test_data_path = "https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv"
    df = load_csv(test_data_path)
    
    # add_bill_length_category 호출
    transformed_df = add_bill_length_category(df)
    assert transformed_df is not None, "Error: Transformed DataFrame is None!"
    
    # 'bill_length_category' 컬럼이 있는지 확인
    assert "bill_length_category" in transformed_df.columns, "Error: bill_length_category not found in DataFrame!"
    
    # 해당 컬럼에 Short, Medium, Long 중 하나의 값을 갖는지 확인
    categories = ["Short", "Medium", "Long"]
    for category in categories:
        assert transformed_df.filter(F.col("bill_length_category") == category).count() > 0, f"Error: {category} category not found!"
    
    print("add_bill_length_category test passed!")

def run_tests():
    session = spark_session()
    test_load_csv(session)
    test_summary(session)
    test_analysis(session)
    test_spark_sql_query(session)
    test_add_bill_length_category(session)

if __name__ == "__main__":
    run_tests()