import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, desc

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

# Lưu lại stdout gốc
original_stdout = sys.stdout
sys.stdout = Tee("bai10.txt", "w")

print("\n========== BÀI 10: XẾP HẠNG SELLER THEO DOANH THU & ĐƠN HÀNG ==========\n")

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai10") \
    .getOrCreate()

try:
    # 1. Đọc dữ liệu bảng Order_Items (chứa Seller_ID, Order_ID, Price)
    order_items_df = spark.read.csv("hdfs://localhost:9000/lab4/Order_Items.csv", header=True, inferSchema=True, sep=";")

    # 2. Xử lý dữ liệu:
    # - Gom nhóm theo Seller_ID
    # - Tính Tổng doanh thu (sum của Price)
    # - Tính Tổng số đơn hàng duy nhất (countDistinct của Order_ID)
    seller_rankings_df = order_items_df.groupBy("Seller_ID") \
        .agg(
            sum("Price").alias("Total_Revenue"),
            countDistinct("Order_ID").alias("Order_Count")
        )

    # 3. Xếp hạng: Ưu tiên Doanh thu giảm dần, nếu bằng nhau thì ưu tiên Số lượng đơn hàng giảm dần
    final_ranked_df = seller_rankings_df.orderBy(desc("Total_Revenue"), desc("Order_Count"))

    # 4. Hiển thị kết quả (Top 20 người bán xuất sắc nhất)
    print("Top 20 Sellers dựa trên Tổng doanh thu và Số lượng đơn hàng:")
    # Format số cho doanh thu có 2 chữ số thập phân
    final_ranked_df.withColumn("Total_Revenue", col("Total_Revenue").cast("decimal(15,2)")) \
        .show(20, truncate=False)

    print("\n=======================================================================")

finally:
    # 5. Dọn dẹp tài nguyên
    sys.stdout.file.close()
    sys.stdout = original_stdout
    spark.stop()