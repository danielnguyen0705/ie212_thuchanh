# 1. Thiết lập tiếng Việt và Môi trường
chcp 65001 > $null
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

$env:JAVA_HOME = "D:\PROGRA~1\Java\jdk-11.0.22+7"
$env:HADOOP_HOME = "D:\PROGRA~1\BigData\Hadoop"
$env:SPARK_HOME = "D:\PROGRA~1\BigData\Spark"
$PYTHON_PATH = "D:\Daniel_Nguyen\UIT_HK6\venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $PYTHON_PATH
$env:PYSPARK_DRIVER_PYTHON = $PYTHON_PATH
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:SPARK_HOME\bin;" + $env:Path

stop-all.cmd > $null 2>&1
taskkill /F /IM java.exe /T > $null 2>&1

# XÓA SẠCH FOLDER DATA TRƯỚC KHI FORMAT
$HADOOP_DATA = "D:\PROGRA~1\BigData\Hadoop\data"
if (Test-Path $HADOOP_DATA) { Remove-Item -Recurse -Force $HADOOP_DATA }
if (Test-Path "C:\tmp") { Remove-Item -Recurse -Force "C:\tmp" }

#Khởi động Hadoop và Spark
hdfs namenode -format -force
start-dfs.cmd
start-yarn.cmd

Start-Sleep -Seconds 30
hdfs dfsadmin -safemode leave

#Đẩy dữ liệu lên HDFS
hdfs dfs -mkdir -p /user/daniel/fecom_data
Get-ChildItem "D:\Daniel_Nguyen\UIT_HK6\IE212_CongNgheDuLieuLon\ThucHanh\Lab4\input\*.csv" | ForEach-Object {
    hdfs dfs -put -f $_.FullName /user/daniel/fecom_data/
}

#Thực thi bài 1
Set-Location "$PSScriptRoot\.."

# Chạy và in kết quả
spark-submit.cmd --master local[*] bai1.py 2>$null | Tee-Object -FilePath "output.txt"