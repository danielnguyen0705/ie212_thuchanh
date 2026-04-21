import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 9: Customer Segmentation") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")
        df_customers = spark.read.csv(hdfs_path + "Customer_List.csv", header=True, inferSchema=True, sep=";")

        df_orders.createOrReplaceTempView("orders")
        df_items.createOrReplaceTempView("items")
        df_customers.createOrReplaceTempView("customers")

        #Truy vấn SQL:
        # - Join 3 bảng để lấy đủ thông tin
        # - Tính: Số đơn (Frequency), Tổng chi tiêu, Giá trị TB (Monetary)
        # - Phân nhóm dựa trên logic đơn giản
        query = """
            WITH CustomerMetrics AS (
                SELECT 
                    c.Subscriber_ID,
                    COUNT(DISTINCT o.Order_ID) AS `So_Luong_Don`,
                    ROUND(AVG(i.Price + i.Freight_Value), 2) AS `Gia_Tri_Trung_Binh`,
                    ROUND(SUM(i.Price + i.Freight_Value), 2) AS `Tong_Chi_Tieu`
                FROM customers c
                JOIN orders o ON c.Customer_Trx_ID = o.Customer_Trx_ID
                JOIN items i ON o.Order_ID = i.Order_ID
                GROUP BY c.Subscriber_ID
            )
            SELECT 
                Subscriber_ID AS `Mã khách hàng`,
                So_Luong_Don AS `Số lượng đơn`,
                Gia_Tri_Trung_Binh AS `Giá trị TB đơn`,
                Tong_Chi_Tieu AS `Tổng chi tiêu`,
                CASE 
                    WHEN So_Luong_Don >= 3 AND Tong_Chi_Tieu > 500 THEN 'Khách hàng VIP'
                    WHEN So_Luong_Don >= 2 THEN 'Khách hàng thân thiết'
                    ELSE 'Khách hàng tiềm năng'
                END AS `Nhóm khách hàng`
            FROM CustomerMetrics
            ORDER BY So_Luong_Don DESC, Tong_Chi_Tieu DESC
        """
        
        result_df = spark.sql(query)

        print("BÁO CÁO PHÂN NHÓM KHÁCH HÀNG (DỰA TRÊN TẦN SUẤT VÀ GIÁ TRỊ)")
        total_rows = result_df.count()
        result_df.show(n=total_rows, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()