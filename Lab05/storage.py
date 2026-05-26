import socket
import json

from datetime import datetime

import pandas as pd
import os

HOST = "0.0.0.0"
PORT = 6200

os.makedirs(
    "output",
    exist_ok=True
)

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
    "Waiting processor..."
)

conn, addr = server.accept()

print(
    addr
)

records = []

while True:

    data = conn.recv(
        4096
    )

    if not data:
        break

    lines = data.decode().split(
        "\n"
    )

    for line in lines:

        if line == "":
            continue

        obj = json.loads(
            line
        )

        obj[
            "timestamp"
        ] = str(
            datetime.now()
        )

        records.append(
            obj
        )

        df = pd.DataFrame(
            records
        )

        df.to_parquet(

            "output/people_result.parquet",

            index=False

        )

        print(
            obj
        )