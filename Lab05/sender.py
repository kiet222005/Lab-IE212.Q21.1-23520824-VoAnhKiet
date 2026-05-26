import cv2
import socket
import struct

SERVER_HOST = "172.23.86.114"   
SERVER_PORT = 6100

client = socket.socket(
    socket.AF_INET,
    socket.SOCK_STREAM
)

client.connect(
    (
        SERVER_HOST,
        SERVER_PORT
    )
)

print(
    "Connected processor"
)

cap = cv2.VideoCapture(
    0,
    cv2.CAP_DSHOW
)

if not cap.isOpened():

    print(
        "Cannot open webcam"
    )

    client.close()

    exit()

try:

    while True:

        ret, frame = cap.read()

        # webcam bị tắt / mất
        if not ret:

            print(
                "Webcam closed"
            )

            break

        ok, encoded = cv2.imencode(
            ".jpg",
            frame
        )

        if not ok:

            print(
                "Encode fail"
            )

            break

        image_bytes = encoded.tobytes()

        try:

            client.sendall(

                struct.pack(
                    ">L",
                    len(
                        image_bytes
                    )
                )

                +

                image_bytes

            )

        except Exception as e:

            print(
                "Connection lost:",
                e
            )

            break

        cv2.imshow(
            "Producer Camera",
            frame
        )

        key = cv2.waitKey(1)

        # nhấn q
        if key == ord(
            'q'
        ):

            print(
                "Stop sender"
            )

            break

        # user bấm X đóng cửa sổ
        if (
            cv2.getWindowProperty(
                "Producer Camera",
                cv2.WND_PROP_VISIBLE
            ) < 1
        ):

            print(
                "Window closed"
            )

            break

finally:

    print(
        "Release camera..."
    )

    cap.release()

    client.close()

    cv2.destroyAllWindows()

    print(
        "Sender stopped"
    )