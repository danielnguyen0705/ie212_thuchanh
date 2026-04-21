import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 8") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")

        df_orders.createOrReplaceTempView("orders")
        df_items.createOrReplaceTempView("items")

        #Truy vấn SQL:
        # - datediff > 0: Giao thực tế SAU hạn chót -> Muộn
        # - datediff <= 0: Giao thực tế TRƯỚC hoặc ĐÚNG hạn chót -> Sớm
        # - Dùng ABS() để lấy giá trị tuyệt đối của số ngày chênh lệch
        query = """
            SELECT 
                o.Order_ID AS `Mã đơn hàng`,
                o.Order_Delivered_Carrier_Date AS `Ngày giao thực tế`,
                i.Shipping_Limit_Date AS `Hạn chót cam kết`,
                CASE 
                    WHEN datediff(o.Order_Delivered_Carrier_Date, i.Shipping_Limit_Date) > 0 THEN 'Muộn'
                    ELSE 'Sớm'
                END AS `Tình trạng`,
                ABS(datediff(o.Order_Delivered_Carrier_Date, i.Shipping_Limit_Date)) AS `Chênh lệch (Ngày)`
            FROM orders o
            JOIN items i ON o.Order_ID = i.Order_ID
            WHERE o.Order_Delivered_Carrier_Date IS NOT NULL 
              AND i.Shipping_Limit_Date IS NOT NULL
            ORDER BY `Tình trạng` DESC, `Chênh lệch (Ngày)` DESC
        """
        
        result_df = spark.sql(query)

        print("BÁO CÁO CHI TIẾT HIỆU SUẤT GIAO HÀNG")
        total_rows = result_df.count()
        result_df.show(n=total_rows, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()