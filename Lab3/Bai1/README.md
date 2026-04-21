**Bài 1**: Tính điểm trung bình và tổng số lượt đánh giá cho mỗi phim
* **Mục tiêu**:
  * Tính điểm trung bình của mỗi phim.
  * Đếm tổng số lượt đánh giá.
  * Tìm phim có điểm trung bình cao nhất (chỉ xét những phim có ít nhất 50 lượt đánh giá).
* **Giải pháp**:
  * Bước 1: Đọc file movies.txt và tạo một map (MovieID → Title).
  * Bước 2: Đọc file ratings_1.txt và ratings_2.txt, map MovieID → (Rating, 1).
  * Bước 3: Reduce để tính tổng điểm và số lượt đánh giá.
  * Bước 4: Tính điểm trung bình, lọc ra phim có ít nhất 5 lượt đánh giá.
  * Bước 5: Tìm phim có điểm trung bình cao nhất.