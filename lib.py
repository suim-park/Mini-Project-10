from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def init_spark():
    spark = SparkSession.builder \
        .appName("PySpark Large Dataset Processing") \
        .getOrCreate()
    return spark

def load_csv(data_path):
    spark = init_spark()
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    return df

def summary(data):
    summary = data.describe()
    summary.show()

def analysis(data):
    grouped_data = df.groupBy("species").agg(
    F.avg("bill_length_mm").alias("avg_bill_length_mm"),
    F.avg("bill_depth_mm").alias("avg_bill_depth_mm"),
    F.avg("flipper_length_mm").alias("avg_flipper_length_mm"),
    F.avg("body_mass_g").alias("avg_body_mass_g")
    )
    grouped_data.show()

def spark_sql_query(data):
    # 일시적 뷰 생성
    data.createOrReplaceTempView("penguins")
    
    # Spark SQL을 사용하여 쿼리 실행
    result = data.spark.sql("""
        SELECT species, 
               MAX(bill_length_mm) as max_bill_length, 
               MIN(bill_length_mm) as min_bill_length
        FROM penguins
        GROUP BY species
    """)
    result.show()

def add_bill_length_category(data):
    # bill_length_mm 컬럼을 기준으로 bill_length_category 컬럼 추가
    conditions = [
        (F.col("bill_length_mm") < 40, "Short"),
        (F.col("bill_length_mm") >= 40 & (F.col("bill_length_mm") < 50), "Medium"),
        (F.col("bill_length_mm") >= 50, "Long")
    ]

    return data.withColumn("bill_length_category", F.when(conditions[0][0], conditions[0][1])
                                                    .when(conditions[1][0], conditions[1][1])
                                                    .otherwise(conditions[2][1]))