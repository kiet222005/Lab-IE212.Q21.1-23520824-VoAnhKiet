import pandas as pd
import json
import numpy as np
import os

df = pd.read_parquet("output/people_result.parquet")

data = df.head(100).to_dict(orient="records")

def convert(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Type {type(obj)} not serializable")

os.makedirs("output", exist_ok=True)

output_file_path = "output/people_output.json"

with open(output_file_path, "w", encoding="utf-8") as f:
    json.dump(
        data, 
        f, 
        indent=4, 
        ensure_ascii=False, 
        default=convert
    )

print(f"Đã xuất dữ liệu thành công ra file JSON tại: {output_file_path}")