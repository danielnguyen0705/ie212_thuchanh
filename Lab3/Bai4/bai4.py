from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("AgeGroupRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Phân loại UserID -> Age Group
def get_age_group(age):
    age = int(age)
    if age < 18: return "Under 18"
    elif age <= 24: return "18-24"
    elif age <= 34: return "25-34"
    elif age <= 44: return "35-44"
    elif age <= 49: return "45-49"
    elif age <= 55: return "50-55"
    else: return "56+"

users_rdd = sc.textFile(f"{data_path}/users.txt")

def parse_user_age(line):
    parts = line.split(',')
    user_id = parts[0]
    age_group = get_age_group(parts[2]) # Cột thứ 3 là tuổi
    return (user_id, age_group)

user_age_dict = dict(users_rdd.map(parse_user_age).collect())

# Đọc movies.txt để lấy tên phim
movies_rdd = sc.textFile(f"{data_path}/movies.txt")
movie_title_dict = dict(movies_rdd.map(lambda l: (l.split(',')[0], l.split(',')[1])).collect())

# Join với Ratings và Tính trung bình
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = r1_rdd.union(r2_rdd)

def map_age_rating(line):
    parts = line.split(',')
    u_id, m_id, rat = parts[0], parts[1], float(parts[2])
    age_grp = user_age_dict.get(u_id, "Unknown")
    return ((m_id, age_grp), (rat, 1))

# ReduceByKey -> MapValues (Avg) -> SortBy (Điểm TB giảm dần)
avg_age_rdd = ratings_rdd.map(map_age_rating) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

# Sắp xếp theo Điểm TB giảm dần
sorted_data = avg_age_rdd.sortBy(lambda x: x[1][0], ascending=False).collect()

header = []
header.append("Phân tích điểm đánh giá theo nhóm tuổi (Sắp xếp theo điểm trung bình giảm dần)")
header.append(f"{'Điểm TB':<10} | {'Nhóm tuổi':<12} | {'ID Phim':<8} | {'Số lượt':<8} | {'Tên phim'}")

print("\n" + "\n".join(header))

list_data = []
for (m_id, age_grp), stats in sorted_data:
    m_title = movie_title_dict.get(m_id, "Unknown")
    row = f"{stats[0]:<10} | {age_grp:<12} | {m_id:<8} | {stats[1]:<8} | {m_title}"
    print(row)
    list_data.append(row)

final_data = header + list_data
sc.parallelize(final_data, 1).saveAsTextFile(f"hdfs://localhost:9000/user/daniel/movie_output_bai4")

sc.stop()