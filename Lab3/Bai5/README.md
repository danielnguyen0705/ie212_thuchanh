Bài 5: Phân Tích Đánh Giá Theo Occupation (Nghề nghiệp) Của Người Dùng
* Mục tiêu:
  * Tính trung bình rating và tổng số lượt đánh giá cho từng Occupation.
* Giải pháp:
  * Tạo dictionary từ users.txt với mapping UserID → Occupation.
  * Với mỗi rating, gán thông tin Occupation theo UserID.
  * Phát hành cặp key-value với key là Occupation và value là (rating, 1).
  * Reduce để tính tổng điểm và số lượt cho mỗi Occupation, sau đó tính trung bình rating.