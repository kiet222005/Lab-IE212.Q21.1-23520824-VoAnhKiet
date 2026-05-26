import socket
import struct
import json

import cv2
import numpy as np

from ultralytics import YOLO
from pyspark.sql import SparkSession

HOST = "0.0.0.0"
PORT = 6100

STORE_HOST = "localhost"
STORE_PORT = 6200

# Khởi tạo SparkSession nguyên bản
spark = SparkSession \
    .builder \
    .appName("PeopleCounter") \
    .getOrCreate()

print("Spark started")

model = YOLO("yolov8n.pt")

server = socket.socket(
    socket.AF_INET,
    socket.SOCK_STREAM
)

server.bind(
    (
        HOST,
        PORT
    )
)

server.listen(1)

print(
    "Waiting producer..."
)

conn, addr = server.accept()

print(
    "Producer:",
    addr
)

storage = socket.socket(
    socket.AF_INET,
    socket.SOCK_STREAM
)

storage.connect(
    (
        STORE_HOST,
        STORE_PORT
    )
)

while True:

    size_data = conn.recv(
        4
    )

    if not size_data:
        break

    image_size = struct.unpack(
        ">L",
        size_data
    )[0]

    image_data = b''

    while len(
        image_data
    ) < image_size:

        packet = conn.recv(
            image_size
            -
            len(
                image_data
            )
        )

        if not packet:
            break

        image_data += packet

    image_array = np.frombuffer(
        image_data,
        dtype=np.uint8
    )

    frame = cv2.imdecode(
        image_array,
        cv2.IMREAD_COLOR
    )

    results = model(
        frame
    )

    objects = []

    people_count = 0

    for result in results:

        boxes = result.boxes

        for box in boxes:

            cls = int(
                box.cls[0]
            )

            label = model.names[
                cls
            ]

            x1,y1,x2,y2 = map(
                int,
                box.xyxy[0]
            )

            if label == "person":

                people_count += 1

            objects.append({

                "class":
                label,

                "bbox":[

                    x1,
                    y1,
                    x2,
                    y2

                ]

            })

    payload = {

        "people_count":
        people_count,

        "objects":
        objects

    }

    storage.sendall(

        (
            json.dumps(
                payload
            )
            +
            "\n"

        ).encode()

    )

    print(
        payload
    )