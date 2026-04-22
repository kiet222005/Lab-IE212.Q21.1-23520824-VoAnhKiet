import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, asc

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
sys.stdout = Tee("bai5.txt", "w")

print("\n========== BÀI 5: THỐNG KÊ ĐIỂM ĐÁNH GIÁ ==========\n")

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai5") \
    .getOrCreate()

try:
    # 1. Đọc dữ liệu bảng Order_Reviews
    reviews_df = spark.read.csv("hdfs://localhost:9000/lab4/Order_Reviews.csv", header=True, inferSchema=True, sep=";")

    # 2. TIỀN XỬ LÝ (QUAN TRỌNG): Loại bỏ giá trị NULL và Ngoại lệ (Outliers)
    # Giả định điểm hợp lệ nằm trong khoảng từ 1 đến 5.
    cleaned_reviews_df = reviews_df \
        .filter(col("Review_Score").isNotNull()) \
        .filter((col("Review_Score") >= 1) & (col("Review_Score") <= 5))

    # 3. Yêu cầu 1: Tính điểm đánh giá trung bình
    # Dùng collect()[0][0] để bóc tách con số ra khỏi cấu trúc Row của DataFrame
    avg_score = cleaned_reviews_df.agg(avg("Review_Score")).collect()[0][0]
    
    # In ra màn hình và làm tròn 2 chữ số thập phân
    print(f"-> Điểm đánh giá trung bình của toàn hệ thống: {avg_score:.2f} / 5.00\n")

    # 4. Yêu cầu 2: Số lượng đánh giá theo từng mức (1 đến 5)
    reviews_distribution_df = cleaned_reviews_df \
        .groupBy("Review_Score") \
        .agg(count("*").alias("Total_Reviews")) \
        .orderBy(asc("Review_Score"))

    print("-> Phân bố số lượng đánh giá theo từng mức điểm:")
    reviews_distribution_df.show()

    print("\n===================================================")

finally:
    # 5. Dọn dẹp tài nguyên
    sys.stdout.file.close()
    sys.stdout = original_stdout
    spark.stop()