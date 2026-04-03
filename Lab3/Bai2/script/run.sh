#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

LOCAL_INPUT="Lab3/input"
LOCAL_OUTPUT="/home/daniel/ThucHanhBigData/Lab3/Bai2/output"
HDFS_INPUT="/user/daniel/movie_data"
HDFS_OUTPUT="/user/daniel/movie_output_bai2"

sudo service ssh start
stop-all.sh
rm -rf /tmp/hadoop-daniel
hdfs namenode -format
start-dfs.sh
hdfs dfsadmin -safemode wait
hdfs dfsadmin -safemode leave

sleep 5

echo "Đang dọn dẹp và chuẩn bị HDFS..."
hdfs dfs -rm -r -f $HDFS_INPUT
hdfs dfs -rm -r -f $HDFS_OUTPUT
rm -rf $LOCAL_OUTPUT

hdfs dfs -mkdir -p $HDFS_INPUT
mkdir -p $LOCAL_OUTPUT

echo "Đang đẩy dữ liệu từ $LOCAL_INPUT lên HDFS..."
hdfs dfs -put $LOCAL_INPUT/movies.txt $HDFS_INPUT/
hdfs dfs -put $LOCAL_INPUT/ratings_1.txt $HDFS_INPUT/
hdfs dfs -put $LOCAL_INPUT/ratings_2.txt $HDFS_INPUT/

echo "Đang thực thi mã PySpark..."
spark-submit Lab3/Bai2/bai2.py


hdfs dfs -getmerge $HDFS_OUTPUT $LOCAL_OUTPUT/ket_qua.txt

echo "Kết quả đã được lưu tại: $LOCAL_OUTPUT/ket_qua.txt"
