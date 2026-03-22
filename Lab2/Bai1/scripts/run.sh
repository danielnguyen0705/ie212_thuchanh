#!/bin/bash

# Tự động biên dịch và đóng gói UDF mỗi khi chạy
rm -rf build
mkdir -p build
# Thêm tham số ép phiên bản dịch về Java 8 để tương thích với Hadoop
javac -source 1.8 -target 1.8 -cp "lib/*:$(hadoop classpath)" -d build/ src/bigdata/CleanReview.java
jar -cf build/preprocess.jar -C build/ bigdata
echo "-> Biên dịch thành công preprocess.jar!"

# Khai báo thư mục
HDFS_INPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/input'
HDFS_OUTPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/Bai1/output'

# Đường dẫn thư mục input trên máy local của bạn
LOCAL_INPUT_DIR='/home/daniel/ThucHanhBigData/Lab2/input'

# Chuẩn bị dữ liệu trên HDFS
hdfs dfs -mkdir -p $HDFS_INPUT_DIR
# Upload dữ liệu từ máy local lên HDFS 
hdfs dfs -put -f $LOCAL_INPUT_DIR/hotel-review.csv $HDFS_INPUT_DIR/
hdfs dfs -put -f $LOCAL_INPUT_DIR/stopwords.txt $HDFS_INPUT_DIR/

# Xóa output cũ (nếu có) để MapReduce không báo lỗi
hdfs dfs -rm -r $HDFS_OUTPUT_DIR || true

# Chạy Pig script
pig -x mapreduce scripts/bai1.pig

#  Kiểm tra kết quả
hdfs dfs -cat $HDFS_OUTPUT_DIR/part-* | head -n 10

hdfs dfs -get /home/daniel/ThucHanhBigData/Lab2/Bai1/output/part-* output/