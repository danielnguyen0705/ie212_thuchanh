-- Đọc dữ liệu gốc 
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

--  TÌM KHÍA CẠNH CÓ NHIỀU ĐÁNH GIÁ TIÊU CỰC  NHẤT
-- Lọc lấy các đánh giá tiêu cực
negative_data = FILTER data BY sentiment == 'negative';
-- Gom nhóm theo khía cạnh
grouped_neg_aspect = GROUP negative_data BY aspect;
-- Đếm số lượng
count_neg_aspect = FOREACH grouped_neg_aspect GENERATE group AS aspect, COUNT(negative_data) AS total_negative;
-- Sắp xếp giảm dần và lấy top 1
ordered_neg = ORDER count_neg_aspect BY total_negative DESC;
top_1_negative = LIMIT ordered_neg 1;

-- TÌM KHÍA CẠNH CÓ NHIỀU ĐÁNH GIÁ TÍCH CỰC NHẤT
-- Lọc lấy các đánh giá tích cực
positive_data = FILTER data BY sentiment == 'positive';
-- Gom nhóm theo khía cạnh
grouped_pos_aspect = GROUP positive_data BY aspect;
-- Đếm số lượng
count_pos_aspect = FOREACH grouped_pos_aspect GENERATE group AS aspect, COUNT(positive_data) AS total_positive;
-- Sắp xếp giảm dần và lấy top 1
ordered_pos = ORDER count_pos_aspect BY total_positive DESC;
top_1_positive = LIMIT ordered_pos 1;

--  LƯU KẾT QUẢ RA HDFS 
STORE top_1_negative INTO '/home/daniel/ThucHanhBigData/Lab2/Bai3/output/negative' USING PigStorage('\t');
STORE top_1_positive INTO '/home/daniel/ThucHanhBigData/Lab2/Bai3/output/positive' USING PigStorage('\t');