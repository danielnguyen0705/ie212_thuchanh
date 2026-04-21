from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("GenreRatingAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

data_path = "hdfs://localhost:9000/user/daniel/movie_data"

# Tạo dictionary MovieID -> List of Genres
movies_rdd = sc.textFile(f"{data_path}/movies.txt")

def parse_movie_genres(line):
    parts = line.split(',')
    movie_id = parts[0]
    genres = parts[2].split('|')
    return (movie_id, genres)

# Lưu thành dict
movie_genre_dict = dict(movies_rdd.map(parse_movie_genres).collect())

#Map từ MovieID -> Rating -> (Genre, Rating)
r1_rdd = sc.textFile(f"{data_path}/ratings_1.txt")
r2_rdd = sc.textFile(f"{data_path}/ratings_2.txt")
ratings_rdd = r1_rdd.union(r2_rdd)

def map_to_genre_rating(line):
    parts = line.split(',')
    movie_id = parts[1]
    rating = float(parts[2])
    
    genres = movie_genre_dict.get(movie_id, ["Unknown"]) # Lấy danh sách thể loại của phim
    
    results = []
    # Phân phối điểm số này cho TỪNG thể loại của phim
    for genre in genres:
        results.append((genre, (rating, 1)))
        
    return results

# Dùng flatMap vì 1 dòng đánh giá có thể sinh ra nhiều bộ 
genre_rating_rdd = ratings_rdd.flatMap(map_to_genre_rating)

#Tính trung bình điểm cho từng thể loại
# Cộng dồn điểm và số lượt đánh giá theo thể loại
reduced_genre_ratings = genre_rating_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Tính điểm TB = Tổng điểm / Số lượt
avg_genre_ratings = reduced_genre_ratings.mapValues(lambda x: (round(x[0] / x[1], 2), x[1]))

# Sắp xếp giảm dần theo điểm trung bình
sorted_genres = avg_genre_ratings.sortBy(lambda x: x[1][0], ascending=False)
all_genres_stats = sorted_genres.collect()

print("Thống kê điểm trung bình theo thể loại")
print(f"{'Thể loại':<10} | {'Điểm TB':<8} | {'Số lượt'}")

for genre, stats in all_genres_stats:
    print(f"{genre:<10} | {stats[0]:<8} | {stats[1]}")

header_info = []
header_info.append("Thống kê điểm trung bình theo thể loại")
header_info.append(f"{'Thể loại':<10} | {'Điểm TB':<8} | {'Số lượt'}")

def format_row(x):
    genre = x[0]
    avg_score = x[1][0]
    count = x[1][1]
    return f"{genre:<10} | {avg_score:<8} | {count}"

list_data = sorted_genres.map(format_row).collect()
final_data = header_info + list_data
final_output_rdd = sc.parallelize(final_data, 1)

# Lưu xuống HDFS
output_path = "hdfs://localhost:9000/user/daniel/movie_output_bai2"
final_output_rdd.saveAsTextFile(output_path)

sc.stop()