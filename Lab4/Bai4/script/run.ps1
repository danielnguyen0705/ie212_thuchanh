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


$HADOOP_DATA = "D:\PROGRA~1\BigData\Hadoop\data"
if (Test-Path $HADOOP_DATA) { Remove-Item -Recurse -Force $HADOOP_DATA }


hdfs namenode -format -force > $null 2>&1
Start-Process cmd.exe "/c start-dfs.cmd" -WindowStyle Hidden
Start-Process cmd.exe "/c start-yarn.cmd" -WindowStyle Hidden

Start-Sleep -Seconds 30
hdfs dfsadmin -safemode leave > $null 2>&1
n
hdfs dfs -mkdir -p /user/daniel/fecom_data
$INPUT_PATH = "D:\Daniel_Nguyen\UIT_HK6\IE212_CongNgheDuLieuLon\ThucHanh\Lab4\input"
Get-ChildItem "$INPUT_PATH\*.csv" | ForEach-Object {
    hdfs dfs -put -f $_.FullName /user/daniel/fecom_data/
}

Set-Location "$PSScriptRoot\.."

spark-submit.cmd --master local[*] bai4.py 2>$null | Tee-Object -FilePath "output.txt"

Start-Process cmd.exe "/c stop-all.cmd" -WindowStyle Hidden