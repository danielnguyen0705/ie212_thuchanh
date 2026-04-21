Bài 6: Phân Tích Đánh Giá Theo Thời Gian
* Mục tiêu:
  * Tính tổng số lượt đánh giá và điểm trung bình cho mỗi năm.
* Giải pháp:
  * Đọc dữ liệu ratings (từ cả ratings_1.txt và ratings_2.txt).
  * Sử dụng hàm trợ giúp để chuyển đổi Timestamp (dạng Unix) thành năm (Year).
  * Với mỗi dòng ratings, phát hành cặp key-value với key là năm và value là (rating, 1).
  * Reduce để tính tổng điểm và số lượt cho mỗi năm, sau đó tính trung bình.