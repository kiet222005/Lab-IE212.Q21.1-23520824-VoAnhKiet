from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("Lab3_Bai3_GenderAnalysis").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn thư mục
    input_path = "/user/anhkiet/lab3/bai1/"
    output_path = "/user/anhkiet/lab3/bai3/output_result"

    # --- Tùy chọn (Lấy thêm tên phim cho kết quả dễ đọc) ---
    movies_rdd = sc.textFile(input_path + "movies.txt")
    movie_dict = movies_rdd.map(lambda line: (line.split(',')[0], line.split(',')[1])).collectAsMap()
    movie_bcast = sc.broadcast(movie_dict)

    # --- BƯỚC 1: Tạo map (UserID -> Gender) từ file users.txt ---
    users_rdd = sc.textFile(input_path + "users.txt")
    # Tách dòng, lấy UserID làm key, Gender làm value -> (UserID, Gender)
    users_mapped = users_rdd.map(lambda line: (line.split(',')[0], line.split(',')[1]))

    # --- BƯỚC 2: Join với ratings để thêm thông tin giới tính ---
    ratings_rdd = sc.textFile(input_path + "ratings_*.txt")
    # Lấy UserID làm key để có thể join -> (UserID, (MovieID, Rating))
    ratings_mapped = ratings_rdd.map(lambda line: (line.split(',')[0], (line.split(',')[1], float(line.split(',')[2]))))

    # Thực hiện Join 2 RDD theo key (UserID)
    # Kết quả của join() sẽ có cấu trúc: (UserID, ( (MovieID, Rating), Gender ))
    joined_rdd = ratings_mapped.join(users_mapped)

    # --- BƯỚC 3: Tính trung bình rating cho mỗi phim theo từng giới tính ---
    # a. Thay đổi key thành (MovieID, Gender) và value thành (Rating, 1) để chuẩn bị đếm
    # Biến x tương đương với 1 dòng trong joined_rdd: (UserID, ((MovieID, Rating), Gender))
    # x[1] là phần value: ((MovieID, Rating), Gender)
    # x[1][0][0] là MovieID | x[1][1] là Gender | x[1][0][1] là Rating
    movie_gender_mapped = joined_rdd.map(lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1)))

    # b. ReduceByKey để tính Tổng điểm và Số lượt đánh giá
    reduced_movie_gender = movie_gender_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # c. Tính điểm trung bình (Tổng điểm / Số lượt)
    # Kết quả: ((MovieID, Gender), AvgRating)
    avg_movie_gender = reduced_movie_gender.mapValues(lambda x: x[0] / x[1])

    # Sắp xếp theo MovieID để dữ liệu hiển thị liền kề Nam/Nữ cho từng phim
    sorted_avg = avg_movie_gender.sortByKey()

    # --- ĐỊNH DẠNG VÀ XUẤT KẾT QUẢ ---
    def format_output(item):
        movie_id = item[0][0]
        gender = item[0][1]
        avg_rating = item[1]
        # Tra cứu tên phim
        title = movie_bcast.value.get(movie_id, "Unknown")
        return f"ID: {movie_id:<5} | Giới tính: {gender} | Điểm TB: {avg_rating:.2f} | Phim: {title}"

    formatted_result = sorted_avg.map(format_output)

    # Ghi ra 1 file duy nhất trên HDFS
    formatted_result.coalesce(1).saveAsTextFile(output_path)

    print("\n" + "="*60)
    print(f"[THÀNH CÔNG] Đã tính điểm trung bình phim theo giới tính!")
    print(f"File output được lưu tại HDFS: {output_path}")
    print("="*60 + "\n")

    sc.stop()

if __name__ == "__main__":
    main()