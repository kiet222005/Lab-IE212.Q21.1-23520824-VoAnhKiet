import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct


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


sys.stdout = Tee("bai2.txt", "w")

spark = SparkSession.builder \
    .appName("Fecom_Data_Analysis_Lab4_Bai2") \
    .getOrCreate()


print("\n========== BÀI 2: THỐNG KÊ SỐ LƯỢNG ==========\n")

orders_df = spark.read.csv("hdfs://localhost:9000/lab4/Orders.csv", header=True, inferSchema=True, sep=";")
customer_df = spark.read.csv("hdfs://localhost:9000/lab4/Customer_List.csv", header=True, inferSchema=True, sep=";")
order_items_df = spark.read.csv("hdfs://localhost:9000/lab4/Order_Items.csv", header=True, inferSchema=True, sep=";")

# 1. Thống kê tổng số đơn hàng
# Sử dụng collect()[0][0] để lấy trực tiếp giá trị con số ra khỏi DataFrame kết quả
total_orders = orders_df.select(countDistinct("Order_ID")).collect()[0][0]

# 2. Thống kê số lượng khách hàng duy nhất
total_customers = customer_df.select(countDistinct("Subscriber_ID")).collect()[0][0]

# 3. Thống kê số lượng người bán duy nhất
total_sellers = order_items_df.select(countDistinct("Seller_ID")).collect()[0][0]

# In kết quả ra màn hình (thêm f-string có dấu phẩy để dễ đọc số lớn)
print(f"- Tổng số đơn hàng: {total_orders:,}")
print(f"- Tổng số khách hàng: {total_customers:,}")
print(f"- Tổng số người bán: {total_sellers:,}")
