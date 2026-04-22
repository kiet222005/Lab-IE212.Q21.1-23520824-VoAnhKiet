import sys
from pyspark.sql import SparkSession


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


sys.stdout = Tee("bai1.txt", "w")

print("========== BÀI 1: ĐỌC DỮ LIỆU TỪ HDFS ==========\n")


spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai1") \
    .getOrCreate()

# 2. Đọc dữ liệu từ các file CSV trên HDFS (thư mục /lab4)
customer_df = spark.read.csv("hdfs://localhost:9000/lab4/Customer_List.csv", header=True, inferSchema=True, sep=";")
order_items_df = spark.read.csv("hdfs://localhost:9000/lab4/Order_Items.csv", header=True, inferSchema=True, sep=";")
order_reviews_df = spark.read.csv("hdfs://localhost:9000/lab4/Order_Reviews.csv", header=True, inferSchema=True, sep=";")
orders_df = spark.read.csv("hdfs://localhost:9000/lab4/Orders.csv", header=True, inferSchema=True, sep=";")
products_df = spark.read.csv("hdfs://localhost:9000/lab4/Products.csv", header=True, inferSchema=True, sep=";")

# 3. In cấu trúc (Schema) của DataFrame
print("--- Schema của Customer_List ---")
customer_df.printSchema()

print("\n--- Schema của Orders_Items ---")
order_items_df.printSchema()

print("\n--- Schema của Orders_Reviews ---")
order_reviews_df.printSchema()

print("\n--- Schema của Orders ---")
orders_df.printSchema()

print("\n--- Schema của Products ---")
products_df.printSchema()




# Trả lại stdout về trạng thái bình thường và đóng file
sys.stdout.file.close()
sys.stdout = sys.stdout.stdout

# Dừng SparkSession
spark.stop()