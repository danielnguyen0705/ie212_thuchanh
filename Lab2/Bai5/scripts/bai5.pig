-- Gọi lại file thư viện UDF 
REGISTER '/home/daniel/ThucHanhBigData/Lab2/Bai1/build/preprocess.jar';
DEFINE CLEAN bigdata.CleanReview('/home/daniel/ThucHanhBigData/Lab2/input/stopwords.txt');

-- Đọc dữ liệu gốc
raw_data = LOAD '/home/daniel/ThucHanhBigData/Lab2/input/hotel-review.csv' USING PigStorage(';') AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);
data = FILTER raw_data BY id != 'id';

-- Làm sạch dữ liệu, chuyển thành chữ thường và tách từ
cleaned_data = FOREACH data GENERATE category, FLATTEN(TOKENIZE(LOWER(CLEAN(review)))) AS word;

-- 5 TỪ LIÊN QUAN NHẤT (XUẤT HIỆN NHIỀU NHẤT) THEO TỪNG CATEGORY
-- Gom nhóm theo từng cặp (category, word) để đếm số lần xuất hiện
word_group = GROUP cleaned_data BY (category, word);
word_count = FOREACH word_group GENERATE group.category AS category, group.word AS word, COUNT(cleaned_data) AS freq;

-- Gom nhóm lại theo category để sắp xếp và lọc Top 5
cat_group = GROUP word_count BY category;
top5_relevant = FOREACH cat_group {
    sorted = ORDER word_count BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top.(word, freq);
};

-- LƯU KẾT QUẢ RA HDFS
STORE top5_relevant INTO '/home/daniel/ThucHanhBigData/Lab2/Bai5/output' USING PigStorage('\t');