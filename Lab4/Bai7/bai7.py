import findspark
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

findspark.init(r"D:\PROGRA~1\BigData\Spark")
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Fecom - Bai 7") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    hdfs_path = "hdfs://localhost:9000/user/daniel/fecom_data/"

    try:
        df_items = spark.read.csv(hdfs_path + "Order_Items.csv", header=True, inferSchema=True, sep=";")
        df_reviews = spark.read.csv(hdfs_path + "Order_Reviews.csv", header=True, inferSchema=True, sep=";")
        df_products = spark.read.csv(hdfs_path + "Products.csv", header=True, inferSchema=True, sep=";")

        df_items.createOrReplaceTempView("items")
        df_reviews.createOrReplaceTempView("reviews")
        df_products.createOrReplaceTempView("products")

        # Truy vấn SQL tổng hợp thông tin: Join Items với Products để lấy tên danh mục, Join với Reviews để lấy điểm
        query = """
            SELECT 
                i.Product_ID AS `Mã sản phẩm`,
                p.Product_Category_Name AS `Danh mục`,
                COUNT(i.Order_ID) AS `Số lượng bán`,
                ROUND(AVG(r.Review_Score), 2) AS `Đánh giá TB`
            FROM items i
            LEFT JOIN products p ON i.Product_ID = p.Product_ID
            LEFT JOIN reviews r ON i.Order_ID = r.Order_ID
            GROUP BY i.Product_ID, p.Product_Category_Name
            ORDER BY `Số lượng bán` DESC
        """
        
        result_df = spark.sql(query)

        top_product = result_df.first()
        print(f"SẢN PHẨM CÓ SỐ LƯỢNG BÁN RA CAO NHẤT:")
        print(f"- Mã sản phẩm: {top_product['Mã sản phẩm']}")
        print(f"- Danh mục:    {top_product['Danh mục']}")
        print(f"- Số lượng:    {top_product['Số lượng bán']} đơn hàng")
        print(f"- Đánh giá:    {top_product['Đánh giá TB']} / 5.0")

        print("\nDANH SÁCH CHI TIẾT TẤT CẢ SẢN PHẨM:")
        total_count = result_df.count()
        result_df.show(n=total_count, truncate=False)

    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()