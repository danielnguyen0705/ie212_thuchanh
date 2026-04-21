#!/bin/bash

HDFS_OUTPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/Bai5/output'

# Dọn dẹp HDFS
hdfs dfs -rm -r $HDFS_OUTPUT_DIR || true

# Chạy Pig script
pig -x mapreduce scripts/bai5.pig

# In kết quả
echo "TOP 5 TỪ LIÊN QUAN NHẤT THEO TỪNG PHÂN LOẠI"
hdfs dfs -cat $HDFS_OUTPUT_DIR/part-*

# Xóa thư mục output cũ ở local (nếu có)
rm -rf output

# Tải toàn bộ thư mục output từ HDFS về máy local
hdfs dfs -get /home/daniel/ThucHanhBigData/Lab2/Bai3/output ./