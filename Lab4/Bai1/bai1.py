import findspark
import os
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 1") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        # Đọc dữ liệu (Sử dụng sep=";" cho Fecom data)
        df = spark.read.csv(hdfs_path + "Products.csv", header=True, inferSchema=True, sep=";")
        total = df.count()
        df.show(n=total, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()