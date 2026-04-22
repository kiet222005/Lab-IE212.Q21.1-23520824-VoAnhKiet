import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, month, asc, desc

# --- THỦ THUẬT: Lớp Tee giúp chuyển hướng output ---
class Tee(object):
    def __init__(self, filename, mode):
        self.file = open(filename, mode, encoding="utf-8")
        self.stdout = sys.stdout
        
    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
        
    def flush(self):
        self.file.flush()
        self.stdout.flush()

# Lưu lại stdout gốc để trả về sau khi chạy xong
original_stdout = sys.stdout
sys.stdout = Tee("bai4.txt", "w")

print("\n========== BÀI 4: ĐƠN HÀNG THEO NĂM VÀ THÁNG ==========\n")

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai4") \
    .getOrCreate()

try:
    # 1. Đọc dữ liệu (Chỉ cần bảng Orders)
    orders_df = spark.read.csv("hdfs://localhost:9000/lab4/Orders.csv", header=True, inferSchema=True, sep=";")

    # 2. Xử lý dữ liệu
    # - Bước 2.1: Trích xuất Năm và Tháng từ cột Order_Purchase_Timestamp
    orders_with_date_df = orders_df \
        .filter(col("Order_Purchase_Timestamp").isNotNull()) \
        .withColumn("Order_Year", year("Order_Purchase_Timestamp")) \
        .withColumn("Order_Month", month("Order_Purchase_Timestamp"))

    # - Bước 2.2: Gom nhóm (groupBy), Đếm (count) và Sắp xếp (orderBy)
    orders_by_time_df = orders_with_date_df \
        .groupBy("Order_Year", "Order_Month") \
        .agg(count("Order_ID").alias("Total_Orders")) \
        .orderBy(asc("Order_Year"), desc("Order_Month"))

    # 3. Hiển thị kết quả
    orders_by_time_df.show(100, truncate=False)

    print("\n=======================================================")

finally:
    # 4. Dọn dẹp tài nguyên
    sys.stdout.file.close()
    sys.stdout = original_stdout # Trả lại terminal như cũ
    spark.stop()