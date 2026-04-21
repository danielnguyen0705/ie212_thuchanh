import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 2") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        # Đọc dữ liệu từ HDFS
        df_orders = spark.read.csv(hdfs_path + "Orders.csv", header=True, inferSchema=True, sep=";")
        df_customers = spark.read.csv(hdfs_path + "Customer_List.csv", header=True, inferSchema=True, sep=";")
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")

        total_orders = df_orders.count()
        total_customers = df_customers.select("Subscriber_ID").distinct().count()
        total_sellers = df_items.select("Seller_ID").distinct().count()

        # dùng UNION ALL để ghép các dòng thành một bảng chính thức
        query = f"""
            SELECT 'Tổng số đơn hàng' AS `Chỉ số thống kê`, {total_orders} AS `Giá trị`
            UNION ALL
            SELECT 'Số lượng khách hàng', {total_customers}
            UNION ALL
            SELECT 'Số lượng người bán', {total_sellers}
        """
        df_res = spark.sql(query)

        df_res.show(truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()