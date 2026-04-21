#!/bin/bash

# Khai báo thư mục cho Bài 3
HDFS_OUTPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/Bai3/output'

# Dọn dẹp thư mục output cũ trên HDFS
hdfs dfs -rm -r $HDFS_OUTPUT_DIR || true

# Chạy Pig script
pig -x mapreduce scripts/bai3.pig

# Hiển thị kết quả ra màn hình
echo "KHÍA CẠNH BỊ CHÊ NHIỀU NHẤT CÙNG SỐ LƯỢNG"
hdfs dfs -cat $HDFS_OUTPUT_DIR/negative/part-*


echo "KHÍA CẠNH ĐƯỢC KHEN NHIỀU NHẤT CÙNG SỐ LƯỢNG "

hdfs dfs -cat $HDFS_OUTPUT_DIR/positive/part-*

# Xóa thư mục output cũ ở local (nếu có)
rm -rf output

# Tải toàn bộ thư mục output từ HDFS về máy local
hdfs dfs -get /home/daniel/ThucHanhBigData/Lab2/Bai3/output ./