-- Đọc dữ liệu gốc từ file csv 
raw_data = LOAD '/home/daniel/ThucHanhBigData/Lab2/input/hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- Loại bỏ dòng tiêu đề
data = FILTER raw_data BY id != 'id';

-- Thống kê tần số xuất hiện của từ 

-- Chuyển review thành chữ thường và dùng TOKENIZE để tách thành các từ rời rạc
words = FOREACH data GENERATE FLATTEN(TOKENIZE(LOWER(review))) AS word;
-- Gom nhóm theo từng từ
grouped_words = GROUP words BY word;
-- Đếm số lần xuất hiện của mỗi từ
word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(words) AS freq;
-- Sắp xếp giảm dần theo tần số (freq)
ordered_words = ORDER word_counts BY freq DESC;
-- Lấy 5 từ đứng đầu
top5_words = LIMIT ordered_words 5;



-- Thống kê số bình luận theo phân loại (category)
grouped_category = GROUP data BY category;
count_category = FOREACH grouped_category GENERATE group AS category, COUNT(data) AS total_comments;



-- Thống kê số bình luận theo khía cạnh (aspect)
grouped_aspect = GROUP data BY aspect;
count_aspect = FOREACH grouped_aspect GENERATE group AS aspect, COUNT(data) AS total_comments;


-- LƯU KẾT QUẢ RA HDFS 
STORE top5_words INTO '/home/daniel/ThucHanhBigData/Lab2/Bai2/output/top5' USING PigStorage('\t');
STORE count_category INTO '/home/daniel/ThucHanhBigData/Lab2/Bai2/output/category' USING PigStorage('\t');
STORE count_aspect INTO '/home/daniel/ThucHanhBigData/Lab2/Bai2/output/aspect' USING PigStorage('\t');