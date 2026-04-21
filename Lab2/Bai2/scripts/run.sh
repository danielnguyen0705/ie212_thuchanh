#!/bin/bash

# Khai báo thư mục
HDFS_OUTPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/Bai2/output'

# Xóa output cũ  để không bị lỗi file đã tồn tại
hdfs dfs -rm -r $HDFS_OUTPUT_DIR || true

# Chạy Pig script
pig -x mapreduce scripts/bai2.pig

# Hiển thị kết quả tuyệt đẹp ra màn hình
echo "TOP 5 TỪ XUẤT HIỆN NHIỀU NHẤT"
hdfs dfs -cat $HDFS_OUTPUT_DIR/top5/part-*

echo "THỐNG KÊ THEO PHÂN LOẠI (CATEGORY)"
hdfs dfs -cat $HDFS_OUTPUT_DIR/category/part-*

echo "THỐNG KÊ THEO KHÍA CẠNH (ASPECT)"
hdfs dfs -cat $HDFS_OUTPUT_DIR/aspect/part-*

# Xóa thư mục output cũ ở local (nếu có)
rm -rf output

# Tải toàn bộ thư mục output từ HDFS về máy local
hdfs dfs -get /home/daniel/ThucHanhBigData/Lab2/Bai2/output ./