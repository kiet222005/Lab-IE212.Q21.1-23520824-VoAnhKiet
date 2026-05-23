import cv2
import base64
import json
import time
from kafka import KafkaProducer

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=5242880 # Tăng limit size lên 5MB để chứa vừa base64
    )

def start_streaming():
    producer = get_kafka_producer()
    topic_name = "raw-video-stream"
    camera_id = "cam_01"
    
    # Mở webcam mặc định (đổi 0 thành đường dẫn 'video.mp4' nếu muốn test video)
    cap = cv2.VideoCapture("test_video.mp4")
    print(f"Starting stream for {camera_id}...")

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
            
        # Resize để giảm kích thước payload truyền qua mạng
        frame = cv2.resize(frame, (640, 480))
        
        # Nén JPEG và chuyển sang Base64
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        jpg_as_text = base64.b64encode(buffer).decode('utf-8')
        
        payload = {
            "camera_id": camera_id,
            "timestamp": time.time(),
            "frame_data": jpg_as_text
        }
        
        try:
            producer.send(topic_name, payload)
            print(f"[{time.strftime('%H:%M:%S')}] Sent frame...")
        except Exception as e:
            print(f"Error sending frame: {e}")
            
        # Delay để giả lập ~15 FPS, tránh ngập lụt Kafka khi test
        time.sleep(1/15)

    cap.release()
    producer.close()

if __name__ == "__main__":
    start_streaming()