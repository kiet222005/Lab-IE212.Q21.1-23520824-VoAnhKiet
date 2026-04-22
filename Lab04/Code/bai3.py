
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc


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
sys.stdout = Tee("bai3.txt", "w")

print("\n========== BÀI 3: SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA ==========\n")

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai3") \
    .getOrCreate()

try:
    # 1. Đọc dữ liệu
    orders_df = spark.read.csv("hdfs://localhost:9000/lab4/Orders.csv", header=True, inferSchema=True, sep=";")
    customer_df = spark.read.csv("hdfs://localhost:9000/lab4/Customer_List.csv", header=True, inferSchema=True, sep=";")

    # 2. Xử lý dữ liệu: Join -> GroupBy -> Count -> OrderBy
    orders_by_country_df = orders_df.join(customer_df, on="Customer_Trx_ID", how="inner") \
        .groupBy("Customer_Country") \
        .agg(count("Order_ID").alias("Total_Orders")) \
        .orderBy(desc("Total_Orders"))

    orders_by_country_df.show(50, truncate=False)

    print("\n============================================================")

finally:
    sys.stdout.file.close()
    sys.stdout = original_stdout 
    spark.stop() 