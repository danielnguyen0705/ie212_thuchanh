-- Đăng ký thư viện chứa UDF vừa được build
REGISTER 'build/preprocess.jar';

-- Định nghĩa hàm CLEAN. Nó sẽ nhận đường dẫn file stopwords trên local (máy Ubuntu)
DEFINE CLEAN bigdata.CleanReview('/home/daniel/ThucHanhBigData/Lab2/input/stopwords.txt');

-- Đọc dữ liệu từ HDFS
raw_data = LOAD '/home/daniel/ThucHanhBigData/Lab2/input/hotel-review.csv'
USING PigStorage(';')
AS (
    id:chararray,
    review:chararray,
    aspect:chararray,
    category:chararray,
    sentiment:chararray
);

-- Loại bỏ dòng tiêu đề (header) có id = 'id'
data_no_header = FILTER raw_data BY id != 'id';

-- Tiến hành làm sạch dữ liệu: Đưa id, review (đã qua UDF CLEAN), aspect, category, sentiment vào schema mới
cleaned_data = FOREACH data_no_header GENERATE
    id,
    CLEAN(review) AS clean_review,
    aspect,
    category,
    sentiment;

-- (Tùy chọn) Bỏ comment dòng DUMP dưới đây nếu muốn Pig in kết quả ra màn hình trong lúc chạy
-- DUMP cleaned_data;

-- Lưu kết quả vào thư mục output trên HDFS (mặc định cách nhau bằng tab)
STORE cleaned_data
    INTO '/home/daniel/ThucHanhBigData/Lab2/Bai1/output'
    USING PigStorage('\t');