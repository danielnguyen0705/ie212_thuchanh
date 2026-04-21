-- Gọi lại file thư viện UDF 
REGISTER '/home/daniel/ThucHanhBigData/Lab2/Bai1/build/preprocess.jar';
DEFINE CLEAN bigdata.CleanReview('/home/daniel/ThucHanhBigData/Lab2/input/stopwords.txt');

-- Đọc dữ liệu gốc
raw_data = LOAD '/home/daniel/ThucHanhBigData/Lab2/input/hotel-review.csv' USING PigStorage(';') AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);
data = FILTER raw_data BY id != 'id';

-- Làm sạch dữ liệu trước (để loại bỏ stopword)
cleaned_data = FOREACH data GENERATE category, sentiment, CLEAN(review) AS clean_review;


-- 5 TỪ TÍCH CỰC NHẤT THEO CATEGORY
-- Lọc bình luận tích cực và tách từ
pos_data = FILTER cleaned_data BY sentiment == 'positive';
pos_words = FOREACH pos_data GENERATE category, FLATTEN(TOKENIZE(LOWER(clean_review))) AS word;

-- Gom nhóm theo từng (category, word) để đếm số lần xuất hiện
pos_word_group = GROUP pos_words BY (category, word);
pos_word_count = FOREACH pos_word_group GENERATE group.category AS category, group.word AS word, COUNT(pos_words) AS freq;

-- Gom nhóm lại theo category để lọc Top 5
pos_cat_group = GROUP pos_word_count BY category;
top5_pos = FOREACH pos_cat_group {
    sorted = ORDER pos_word_count BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top.(word, freq);
};


-- 5 TỪ TIÊU CỰC NHẤT THEO CATEGORY

-- Lọc bình luận tiêu cực và tách từ
neg_data = FILTER cleaned_data BY sentiment == 'negative';
neg_words = FOREACH neg_data GENERATE category, FLATTEN(TOKENIZE(LOWER(clean_review))) AS word;

-- Gom nhóm theo từng (category, word) để đếm số lần xuất hiện
neg_word_group = GROUP neg_words BY (category, word);
neg_word_count = FOREACH neg_word_group GENERATE group.category AS category, group.word AS word, COUNT(neg_words) AS freq;

-- Gom nhóm lại theo category để lọc Top 5
neg_cat_group = GROUP neg_word_count BY category;
top5_neg = FOREACH neg_cat_group {
    sorted = ORDER neg_word_count BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top.(word, freq);
};


-- LƯU KẾT QUẢ RA HDFS
STORE top5_pos INTO '/home/daniel/ThucHanhBigData/Lab2/Bai4/output/positive_words' USING PigStorage('\t');
STORE top5_neg INTO '/home/daniel/ThucHanhBigData/Lab2/Bai4/output/negative_words' USING PigStorage('\t');