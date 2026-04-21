from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("MovieRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR") # Ẩn bớt log cảnh báo (WARN) để dễ nhìn kết quả

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Đọc file movies.txt và tạo dictionary (MovieID -> Title)
movies_rdd = sc.textFile(f"{data_path}/movies.txt")

def parse_movie(line):
    parts = line.split(',')
    # Trả về MovieID, Title
    return (parts[0], parts[1])

movie_dict = dict(movies_rdd.map(parse_movie).collect())

# Đọc file ratings, map MovieID -> (Rating, 1)
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")

# Gộp 2 RDD ratings và map dữ liệu
ratings_rdd = r1_rdd.union(r2_rdd)
mapped_ratings = ratings_rdd.map(lambda line: (line.split(',')[1], (float(line.split(',')[2]), 1)))
reduced_ratings = mapped_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
avg_ratings = reduced_ratings.mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

# Sắp xếp theo ID phim
sorted_all_ratings = avg_ratings.sortBy(lambda x: x[1][0], ascending=False)
all_movies_stats = sorted_all_ratings.collect()
print("Danh sách phim sắp xếp theo điểm trung bình")
print(f"{'ID':<4} | {'Tên phim':<60} | {'Điểm TB':<8} | {'Số lượt'}")

for movie_id, stats in all_movies_stats:
    avg_score = stats[0]
    total_votes = stats[1]
    movie_title = movie_dict.get(movie_id, "Không rõ tên phim")
    print(f"{movie_id:<4} | {movie_title:<60} | {avg_score:<8} | {total_votes}")

min_vote = 5
filtered_ratings = avg_ratings.filter(lambda x: x[1][1] >= min_vote) # Lọc những phim có số lượt đánh giá >= 5
# Tìm phim có điểm trung bình cao nhất --> Sắp xếp giảm dần theo điểm trung bình (phần tử x[1][0])
sorted_filtered_ratings = filtered_ratings.sortBy(lambda x: x[1][0], ascending=False)

header_info = []

# Lấy bộ phim đứng đầu
if not sorted_filtered_ratings.isEmpty():
    top_movie = sorted_filtered_ratings.first()
    movie_id = top_movie[0]
    avg_score = top_movie[1][0]
    total_votes = top_movie[1][1]
    movie_title = movie_dict.get(movie_id, "Không rõ tên phim")
    
    print(f"\nPhim được đánh giá cao nhất: '{movie_title}' (ID: {movie_id})")
    print(f"Điểm trung bình: {avg_score} ({total_votes} lượt đánh giá)\n")

    header_info.append(f"Bộ phim xuất sắc nhất là phim '{movie_title}' (ID: {movie_id})")
    header_info.append(f"Điểm TB: {avg_score} | Số lượt: {total_votes}")
else:
    msg = f"Không tìm thấy phim nào thỏa mãn điều kiện >= {min_vote} lượt đánh giá."
    print(f"\n{msg}\n")
    header_info.append(msg)

header_info.append("\nDANH SÁCH TOÀN BỘ PHIM:")
header_info.append(f"{'ID':<4} | {'Tên phim':<60} | {'Điểm TB':<8} | {'Số lượt'}")

def format_row_to_table(x):
    m_id = x[0]
    a_score = x[1][0]
    t_votes = x[1][1]
    m_title = movie_dict.get(m_id, "Không rõ tên phim")
    return f"{m_id:<4} | {m_title:<60} | {a_score:<8} | {t_votes}"

list_data= sorted_all_ratings.map(format_row_to_table).collect()
final_data = header_info + list_data
final_output_rdd = sc.parallelize(final_data, 1)

# Lưu xuống HDFS 
output_path = "hdfs://localhost:9000/user/daniel/movie_output"
final_output_rdd.saveAsTextFile(output_path)

sc.stop()