from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("GenderRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Tạo map (UserID -> Gender)
users_rdd = sc.textFile(f"{data_path}/users.txt")

def parse_user(line):
    parts = line.split(',')
    user_id = parts[0]
    gender = parts[1] 
    return (user_id, gender)

user_gender_dict = dict(users_rdd.map(parse_user).collect())

movies_rdd = sc.textFile(f"{data_path}/movies.txt")
movie_title_dict = dict(movies_rdd.map(lambda l: (l.split(',')[0], l.split(',')[1])).collect())

# Gắn giới tính vào ratings
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = r1_rdd.union(r2_rdd)

def map_gender_rating(line):
    parts = line.split(',')
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    
    gender = user_gender_dict.get(user_id, "U") # U là Unknown
    return ((movie_id, gender), (rating, 1))

gender_mapped_rdd = ratings_rdd.map(map_gender_rating)

# Tính trung bình rating theo giới tính
# Cộng tổng điểm và số lượt theo Key (MovieID, Gender)
reduced_rdd = gender_mapped_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm trung bình
avg_rdd = reduced_rdd.mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

# Sắp xếp theo MovieID để dễ theo dõi
sorted_rdd = avg_rdd.sortByKey()
all_data = sorted_rdd.collect()

header_info = []
header_info.append("Phân tích điểm đánh giá theo giới tính")
header_info.append(f"{'ID':<6} | {'Giới tính':<10} | {'Điểm TB':<8} | {'Số lượt'} | {'Tên phim'}")
header_info.append("-" * 85)

# In Terminal và chuẩn bị data lưu file
print("\n" + "\n".join(header_info))

list_data = []
for (m_id, gender), stats in all_data:
    avg_score = stats[0]
    count = stats[1]
    m_title = movie_title_dict.get(m_id, "Unknown")
    gender_full = "Nam (M)" if gender == 'M' else "Nữ (F)"
    
    row = f"{m_id:<6} | {gender_full:<10} | {avg_score:<8} | {count:<7} | {m_title}"
    print(row)
    list_data.append(row)

final_data = header_info + list_data
final_output_rdd = sc.parallelize(final_data, 1)

output_path = "hdfs://localhost:9000/user/daniel/movie_output_bai3"
final_output_rdd.saveAsTextFile(output_path)

sc.stop()