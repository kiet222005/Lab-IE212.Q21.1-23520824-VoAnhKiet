from pyspark import SparkConf, SparkContext
from datetime import datetime

# Hàm trợ giúp: Chuyển đổi Unix Timestamp sang năm
def extract_year(timestamp_str):
    try:
        # Chuyển chuỗi sang số nguyên và đổi thành đối tượng datetime
        ts = int(timestamp_str)
        return datetime.fromtimestamp(ts).year
    except:
        return "Unknown"

def main():
    conf = SparkConf().setAppName("Lab3_Bai6_TimeAnalysis").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn (Dữ liệu nguồn lấy từ bai1)
    input_path = "/user/anhkiet/lab3/bai1/"
    output_path = "/user/anhkiet/lab3/bai6/output_result"

    # --- BƯỚC 1: Đọc dữ liệu ratings ---
    ratings_rdd = sc.textFile(input_path + "ratings_*.txt")

    # --- BƯỚC 2 & 3: Chuyển đổi Timestamp thành năm và Map (Year, (Rating, 1)) ---
    # Schema ratings: UserID, MovieID, Rating, Timestamp
    def map_rating_to_year(line):
        parts = line.split(',')
        rating = float(parts[2])
        timestamp = parts[3]
        year = extract_year(timestamp)
        return (year, (rating, 1))

    year_ratings_mapped = ratings_rdd.map(map_rating_to_year)

    # --- BƯỚC 4: Reduce để tính tổng điểm và số lượt cho mỗi năm ---
    # Cộng dồn (tổng điểm, tổng số lượt) theo key là năm
    reduced_year_rdd = year_ratings_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Tính trung bình: (Năm, (Trung bình, Tổng lượt))
    avg_year_rdd = reduced_year_rdd.mapValues(lambda x: (x[0] / x[1], x[1]))

    # Sắp xếp theo năm tăng dần
    sorted_year_rdd = avg_year_rdd.sortByKey()

    # --- ĐỊNH DẠNG VÀ XUẤT KẾT QUẢ ---
    formatted_result = sorted_year_rdd.map(
        lambda x: f"Năm: {x[0]} | Điểm TB: {x[1][0]:.2f} | Tổng số lượt đánh giá: {x[1][1]:,}"
    )

    # Lưu ra 1 file duy nhất trên HDFS
    formatted_result.coalesce(1).saveAsTextFile(output_path)

    print("\n" + "="*60)
    print(f"[THÀNH CÔNG] Đã phân tích đánh giá theo năm!")
    print(f"Kết quả lưu tại: {output_path}")
    print("="*60 + "\n")

    sc.stop()

if __name__ == "__main__":
    main()