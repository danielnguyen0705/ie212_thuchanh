import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 5") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_reviews = spark.read.csv(hdfs_path + "Order_Reviews.csv", header=True, inferSchema=True, sep=";")
        
        df_reviews.createOrReplaceTempView("reviews")

        # Truy vấn thống kê số lượng theo từng mức (đã lọc NULL và giá trị ngoại lệ) Chỉ lấy điểm từ 1 đến 5
        query_distribution = """
            SELECT 
                Review_Score AS `Mức điểm`, 
                COUNT(*) AS `Số lượng đánh giá`
            FROM reviews
            WHERE Review_Score IS NOT NULL 
              AND Review_Score BETWEEN 1 AND 5
            GROUP BY Review_Score
            ORDER BY Review_Score DESC
        """
        
        # Truy vấn tính điểm trung bình tổng quát
        query_avg = """
            SELECT 
                'Tất cả' AS `Phân loại`,
                ROUND(AVG(Review_Score), 2) AS `Điểm trung bình hệ thống`
            FROM reviews
            WHERE Review_Score IS NOT NULL 
              AND Review_Score BETWEEN 1 AND 5
        """
        
        print("\nTHỐNG KÊ CHI TIẾT THEO MỨC ĐIỂM")
        spark.sql(query_distribution).show(truncate=False)

        print("ĐIỂM ĐÁNH GIÁ TRUNG BÌNH TOÀN HỆ THỐNG")
        spark.sql(query_avg).show(truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()