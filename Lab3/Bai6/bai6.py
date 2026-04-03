from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf().setAppName("TimeRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Đọc dữ liệu ratings (r1 + r2)
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = r1_rdd.union(r2_rdd)

# Hàm chuyển Timestamp (Unix) -> Năm (Year)
#Timestamp tức là giây thứ n tính từ năm 1970
def get_year_from_timestamp(timestamp_str):
    try:
        ts = int(timestamp_str)
        # dt_object = datetime.fromtimestamp(ts)
        # return str(dt_object.year)
        return datetime.fromtimestamp(ts).strftime('%Y') #Cái này thì gọn hơn
    except:
        return "Unknown"

# Map Year -> (Rating, 1)
def map_time_rating(line):
    parts = line.split(',')
    # Cấu trúc rating: UserID, MovieID, Rating, Timestamp
    rating = float(parts[2])
    year = get_year_from_timestamp(parts[3])
    return (year, (rating, 1))

# Reduce và Tính trung bình
time_stats_rdd = ratings_rdd.map(map_time_rating) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

# Sắp xếp theo Năm tăng dần để thấy sự thay đổi theo thời gian
sorted_time_data = time_stats_rdd.sortByKey(ascending=True).collect()

header = []
header.append("Phân tích điểm đánh giá theo năm")
header.append(f"{'Năm':<4} | {'Điểm TB':<8} | {'Tổng lượt'}")


print("\n" + "\n".join(header))

list_data = []
for year, stats in sorted_time_data:
    row = f"{year:<4} | {stats[0]:<8} | {stats[1]}"
    print(row)
    list_data.append(row)

final_data = header + list_data
sc.parallelize(final_data, 1).saveAsTextFile(f"hdfs://localhost:9000/user/daniel/movie_output_bai6")

sc.stop()