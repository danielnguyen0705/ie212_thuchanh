import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 6") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")
        df_products = spark.read.csv(hdfs_path + "Products.csv", header=True, inferSchema=True, sep=";")

        df_orders.createOrReplaceTempView("orders")
        df_items.createOrReplaceTempView("items")
        df_products.createOrReplaceTempView("products")

        # Truy vấn SQL tính doanh thu năm 2024 theo danh mục: Doanh thu = Price + Freight_Value
        query = """
            SELECT 
                p.Product_Category_Name AS `Danh mục sản phẩm`,
                ROUND(SUM(i.Price + i.Freight_Value), 2) AS `Doanh thu`
            FROM orders o
            JOIN items i ON o.Order_ID = i.Order_ID
            JOIN products p ON i.Product_ID = p.Product_ID
            WHERE year(o.Order_Purchase_Timestamp) = 2024
            GROUP BY p.Product_Category_Name
            ORDER BY `Doanh thu` DESC
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