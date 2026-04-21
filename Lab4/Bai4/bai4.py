import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 4") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        
        df_orders.createOrReplaceTempView("orders")

        # Truy vấn SQL: Tách Năm, Tháng, Đếm và Sắp xếp
        # Năm tăng dần (ASC), Tháng giảm dần (DESC)
        query = """
            SELECT 
                year(Order_Purchase_Timestamp) AS `Năm`,
                month(Order_Purchase_Timestamp) AS `Tháng`,
                COUNT(Order_ID) AS `Số lượng đơn hàng`
            FROM orders
            GROUP BY `Năm`, `Tháng`
            ORDER BY `Năm` ASC, `Tháng` DESC
        """
        
        result_df = spark.sql(query)

        total_rows = result_df.count()
        result_df.show(n=total_rows, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()