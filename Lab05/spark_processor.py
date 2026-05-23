import base64
import cv2
import numpy as np
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from ultralytics import YOLO

# Khởi tạo model YOLOv8 (tự động tải pre-trained weights yolov8n.pt lần đầu)
model = YOLO("yolov8n.pt")

def process_frame_yolo(base64_img):
    try:
        # Giải mã Base64 -> Numpy Array -> Image
        img_data = base64.b64decode(base64_img)
        np_arr = np.frombuffer(img_data, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        
        # Nhận diện class 0 (person)
        results = model(img, classes=0, verbose=False)
        
        person_count = 0
        bboxes = []
        
        for r in results:
            boxes = r.boxes
            for box in boxes:
                b = [round(coord, 2) for coord in box.xyxy[0].tolist()]
                bboxes.append(b)
                person_count += 1
                
        result_dict = {
            "count": person_count,
            "boxes": bboxes
        }
        return json.dumps(result_dict)
    except Exception as e:
        return json.dumps({"count": 0, "boxes": [], "error": str(e)})

def main():
    spark = SparkSession.builder \
        .appName("YoloStreamingKafkaToHDFS") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema cho cục dữ liệu nhận từ Kafka
    kafka_schema = StructType([
        StructField("camera_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("frame_data", StringType(), True)
    ])

    # Đăng ký UDF
    yolo_udf = udf(process_frame_yolo, StringType())

    # Đọc luồng từ Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw-video-stream") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON từ cột value của Kafka
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), kafka_schema).alias("data")) \
        .select("data.*")

    # Chạy model nhận diện và loại bỏ ảnh thô
    processed_df = parsed_df \
        .withColumn("detection_results", yolo_udf(col("frame_data"))) \
        .drop("frame_data")

    # Ghi kết quả xuống Hadoop HDFS
    # Định dạng output là JSON, mỗi batch tạo thành các file trong thư mục HDFS
    query = processed_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/video_stream") \
        .option("path", "hdfs://localhost:9000/data/camera_detections") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()