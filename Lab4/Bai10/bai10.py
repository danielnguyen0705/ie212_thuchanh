import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 10: Seller Ranking") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")
        
        df_items.createOrReplaceTempView("items")

        #Truy vấn SQL: 
        # - Group by Seller_ID
        # - Doanh thu = SUM(Price + Freight_Value)
        # - Số đơn = COUNT(DISTINCT Order_ID)
        # - Sắp xếp theo doanh thu giảm dần
        query = """
            SELECT 
                Seller_ID AS `Mã người bán`,
                COUNT(DISTINCT Order_ID) AS `Số đơn hàng`,
                ROUND(SUM(Price + Freight_Value), 2) AS `Tổng doanh thu`
            FROM items
            GROUP BY Seller_ID
            ORDER BY `Tổng doanh thu` DESC, `Số đơn hàng` DESC
        """
        
        result_df = spark.sql(query)

        print("BẢNG XẾP HẠNG NGƯỜI BÁN THEO DOANH THU VÀ SỐ LƯỢNG ĐƠN HÀNG")
        total_sellers = result_df.count()
        result_df.show(n=total_sellers, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()