from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def init_spark(app_name: str, memory: str = "2g") -> SparkSession:
    session = SparkSession.builder.appName(app_name) \
        .config("session.executor.memory", memory) \
        .getOrCreate()
    return session

def read_csv(session: SparkSession, file_path: str) -> DataFrame:
    data_file = session.read.csv(file_path, header=True, inferSchema=True)
    return data_file

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
    return result

def transform(spark: SparkSession, data: DataFrame) -> DataFrame:
    # bill_length_mm 컬럼을 기준으로 bill_length_category 컬럼 추가
    conditions = [
        (F.col("bill_length_mm") < 40, "Short"),
        ((F.col("bill_length_mm") >= 40) & (F.col("bill_length_mm") < 50), "Medium"),
        (F.col("bill_length_mm") >= 50, "Long")
    ]

    return data.withColumn("bill_length_category", F.when(conditions[0][0], conditions[0][1])
                                                    .when(conditions[1][0], conditions[1][1])
                                                    .otherwise(conditions[2][1]))