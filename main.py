from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col
import pyspark.sql.functions as F

def init_spark(app_name: str, memory: str = "2g") -> SparkSession:
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()
    return spark

def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def spark_sql_query(spark: SparkSession, data: DataFrame):
    # 일시적 뷰 생성
    data.createOrReplaceTempView("penguins")
    
    # Spark SQL을 사용하여 쿼리 실행
    result = spark.sql("""
        SELECT species, 
               MAX(bill_length_mm) as max_bill_length, 
               MIN(bill_length_mm) as min_bill_length
        FROM penguins
        GROUP BY species
    """)
    result.show()

def add_bill_length_category(data: DataFrame) -> DataFrame:
    # bill_length_mm 컬럼을 기준으로 bill_length_category 컬럼 추가
    conditions = [
        (F.col("bill_length_mm") < 40, "Short"),
        ((F.col("bill_length_mm") >= 40) & (F.col("bill_length_mm") < 50), "Medium"),
        (F.col("bill_length_mm") >= 50, "Long")
    ]

    return data.withColumn("bill_length_category", F.when(conditions[0][0], conditions[0][1])
                                                    .when(conditions[1][0], conditions[1][1])
                                                    .otherwise(conditions[2][1]))

                                                    

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
    df_with_category = add_bill_length_category(df)
    print("Data After Adding Bill Length Category:")
    df_with_category.show()