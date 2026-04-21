#!/bin/bash

HDFS_OUTPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/Bai4/output'

# Dọn dẹp HDFS
hdfs dfs -rm -r $HDFS_OUTPUT_DIR || true

# Chạy Pig script
pig -x mapreduce scripts/bai4.pig

# In kết quả
echo "TOP 5 TỪ TÍCH CỰC NHẤT TRONG TỪNG CATEGORY"
hdfs dfs -cat $HDFS_OUTPUT_DIR/positive_words/part-*


echo "TOP 5 TỪ TIÊU CỰC NHẤT TRONG TỪNG CATEGORY "
hdfs dfs -cat $HDFS_OUTPUT_DIR/negative_words/part-*

# Xóa thư mục output cũ ở local (nếu có)
rm -rf output

# Tải toàn bộ thư mục output từ HDFS về máy local
hdfs dfs -get /home/daniel/ThucHanhBigData/Lab2/Bai3/output ./