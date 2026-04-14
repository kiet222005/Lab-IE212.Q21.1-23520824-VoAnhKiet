from pyspark import SparkConf, SparkContext

# Hàm phân loại độ tuổi thành các nhóm
def get_age_group(age_str):
    try:
        age = int(age_str)
        if age < 18:
            return "Dưới 18"
        elif 18 <= age <= 34:
            return "18-34"
        elif 35 <= age <= 49:
            return "35-49"
        else:
            return "50+"
    except ValueError:
        return "Unknown" # Xử lý trường hợp dữ liệu bị lỗi/thiếu

def main():
    conf = SparkConf().setAppName("Lab3_Bai4_AgeAnalysis").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn thư mục (Tái sử dụng input từ bai1)
    input_path = "/user/anhkiet/lab3/bai1/"
    output_path = "/user/anhkiet/lab3/bai4/output_result"

    # --- Tùy chọn: Broadcast tên phim ---
    movies_rdd = sc.textFile(input_path + "movies.txt")
    movie_dict = movies_rdd.map(lambda line: (line.split(',')[0], line.split(',')[1])).collectAsMap()
    movie_bcast = sc.broadcast(movie_dict)

    # --- BƯỚC 1: Tạo map (UserID -> Age Group) từ users.txt ---
    users_rdd = sc.textFile(input_path + "users.txt")
    # Schema users: UserID, Gender, Age, Occupation, Zip-code
    users_mapped = users_rdd.map(lambda line: (line.split(',')[0], get_age_group(line.split(',')[2])))

    # --- BƯỚC 2: Join với ratings để thêm thông tin nhóm tuổi ---
    ratings_rdd = sc.textFile(input_path + "ratings_*.txt")
    # Schema ratings: UserID, MovieID, Rating, Timestamp
    # Đưa về dạng: (UserID, (MovieID, Rating))
    ratings_mapped = ratings_rdd.map(lambda line: (line.split(',')[0], (line.split(',')[1], float(line.split(',')[2]))))

    # Join theo UserID: Kết quả -> (UserID, ( (MovieID, Rating), AgeGroup ))
    joined_rdd = ratings_mapped.join(users_mapped)

    # --- BƯỚC 3: Tính trung bình điểm đánh giá theo nhóm tuổi ---
    # a. Map về dạng key-value mới: ((MovieID, AgeGroup), (Rating, 1))
    movie_age_mapped = joined_rdd.map(lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1)))

    # b. ReduceByKey để tính Tổng điểm & Tổng số lượt
    reduced_rdd = movie_age_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # c. Tính trung bình: (Tổng điểm / Tổng số lượt)
    avg_rdd = reduced_rdd.mapValues(lambda x: x[0] / x[1])

    # Sắp xếp theo MovieID để dễ nhìn
    sorted_avg = avg_rdd.sortByKey()

    # --- ĐỊNH DẠNG VÀ XUẤT KẾT QUẢ ---
    def format_output(item):
        movie_id = item[0][0]
        age_group = item[0][1]
        avg_rating = item[1]
        title = movie_bcast.value.get(movie_id, "Unknown")
        return f"ID: {movie_id:<5} | Nhóm tuổi: {age_group:<8} | Điểm TB: {avg_rating:.2f} | Phim: {title}"

    formatted_result = sorted_avg.map(format_output)

    # Ghi ra 1 file duy nhất trên HDFS
    formatted_result.coalesce(1).saveAsTextFile(output_path)

    print("\n" + "="*60)
    print(f"[THÀNH CÔNG] Đã tính điểm trung bình phim theo nhóm tuổi!")
    print(f"File output được lưu tại HDFS: {output_path}")
    print("="*60 + "\n")

    sc.stop()

if __name__ == "__main__":
    main()