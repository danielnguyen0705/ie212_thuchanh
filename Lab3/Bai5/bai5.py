from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("OccupationRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Đọc file occupation.txt (ID,Name)
occ_file_rdd = sc.textFile(f"{data_path}/occupation.txt")
# Chuyển thành dict: {'1': 'Programmer', '2': 'Doctor', ...}
occ_name_dict = dict(occ_file_rdd.map(lambda l: (l.split(',')[0], l.split(',')[1])).collect())

# Đọc file users để lấy UserID -> OccupationID
users_rdd = sc.textFile(f"{data_path}/users.txt")
user_occ_dict = dict(users_rdd.map(lambda l: (l.split(',')[0], l.split(',')[3])).collect())

# Xử lý Rating và Tính trung bình
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = r1_rdd.union(r2_rdd)

def map_occ_rating(line):
    parts = line.split(',')
    u_id, rat = parts[0], float(parts[2])
    occ_id = user_occ_dict.get(u_id, "0")
    # Lấy tên nghề nghiệp từ dict đã đọc ở trên
    occ_name = occ_name_dict.get(occ_id, "Other/Unknown")
    return (occ_name, (rat, 1))

# Tính toán và sắp xếp theo Điểm TB giảm dần
avg_occ_rdd = ratings_rdd.map(map_occ_rating) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

sorted_occ_data = avg_occ_rdd.sortBy(lambda x: x[1][0], ascending=False).collect()

header = []
header.append("Phân tích điểm đánh giá theo nghề nghiệp")
header.append(f"{'Nghề nghiệp':<25} | {'Điểm TB':<10} | {'Số lượt'}")

print("\n" + "\n".join(header))

list_data = []
for occ_name, stats in sorted_occ_data:
    row = f"{occ_name:<25} | {stats[0]:<10} | {stats[1]}"
    print(row)
    list_data.append(row)

final_data = header + list_data
sc.parallelize(final_data, 1).saveAsTextFile(f"hdfs://localhost:9000/user/daniel/movie_output_bai5")

sc.stop()