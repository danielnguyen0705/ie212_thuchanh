import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 3") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        df_customers = spark.read.csv(hdfs_path + "Customer_List.csv", header=True, inferSchema=True, sep=";")

        # Đăng ký thành các bảng SQL tạm thời
        df_orders.createOrReplaceTempView("orders")
        df_customers.createOrReplaceTempView("customers")

        query = """
            SELECT 
                c.Customer_Country AS `Quốc gia`, 
                COUNT(o.Order_ID) AS `Số lượng đơn hàng`
            FROM orders o
            JOIN customers c ON o.Customer_Trx_ID = c.Customer_Trx_ID
            GROUP BY c.Customer_Country
            ORDER BY `Số lượng đơn hàng` DESC
        """
        
        result_df = spark.sql(query)

        # In kết quả ra console
        total_countries = result_df.count()
        result_df.show(n=total_countries, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()