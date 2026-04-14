from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("Lab3_Bai1").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn trên HDFS
    hdfs_path = "/user/anhkiet/lab3/bai1/"
    output_path = hdfs_path + "output_result"

    # --- BƯỚC 1: Đọc file movies.txt và tạo broadcast map ---
    movies_rdd = sc.textFile(hdfs_path + "movies.txt")
    movie_dict = movies_rdd.map(lambda line: (line.split(',')[0], line.split(',')[1])).collectAsMap()
    movie_bcast = sc.broadcast(movie_dict)

    # --- BƯỚC 2: Đọc file ratings_1 và ratings_2, map MovieID -> (Rating, 1) ---
    ratings_rdd = sc.textFile(hdfs_path + "ratings_*.txt")
    mapped_ratings = ratings_rdd.map(lambda line: (line.split(',')[1], (float(line.split(',')[2]), 1)))

    # --- BƯỚC 3: Reduce để tính tổng điểm và số lượt đánh giá ---
    reduced_ratings = mapped_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # --- BƯỚC 4: Tính điểm trung bình, lọc ra phim có ít nhất 5 lượt đánh giá ---
    MIN_RATINGS = 5
    avg_ratings = reduced_ratings.mapValues(lambda x: (x[0] / x[1], x[1])) \
                                 .filter(lambda x: x[1][1] >= MIN_RATINGS)

    if avg_ratings.isEmpty():
        print(f"\n[!] Không có phim nào đạt đủ {MIN_RATINGS} lượt đánh giá.\n")
    else:
        # Sắp xếp RDD theo điểm trung bình giảm dần
        sorted_ratings = avg_ratings.sortBy(lambda x: x[1][0], ascending=False)

        # Định dạng danh sách phim thành RDD chứa String
        formatted_list_rdd = sorted_ratings.map(
            lambda x: f"{x[0]}, {movie_bcast.value.get(x[0], 'Không rõ tên')}, {x[1][0]:.2f}, {x[1][1]}"
        )

        # --- BƯỚC 5: Tìm phim có điểm trung bình cao nhất và chuẩn bị xuất file ---
        top_movie = sorted_ratings.first()
        m_id = top_movie[0]
        m_avg = top_movie[1][0]
        m_count = top_movie[1][1]
        m_title = movie_bcast.value.get(m_id, "Không rõ tên")

        # Tạo một mảng String chứa kết quả Top 1 để chèn vào cuối file
        top_1_text = [
            "",
            "--------------------------------------------------",
            "KET QUA: PHIM CO DIEM TRUNG BINH CAO NHAT",
            f"ID phim: {m_id}",
            f"Ten phim: {m_title}",
            f"Diem trung binh: {m_avg:.2f} / 5.0",
            f"So luot danh gia: {m_count} luot",
            "--------------------------------------------------"
        ]
        
        # Biến mảng text trên thành một RDD mới
        top_1_rdd = sc.parallelize(top_1_text)

        # Dùng union() để nối RDD danh sách phim với RDD đoạn text kết luận
        final_output_rdd = formatted_list_rdd.union(top_1_rdd)

        # Xuất toàn bộ ra 1 file duy nhất trên HDFS
        final_output_rdd.coalesce(1).saveAsTextFile(output_path)
        
        print(f"\n[THÀNH CÔNG] Đã lưu danh sách và kết quả Top 1 vào file trên HDFS tại: {output_path}\n")

    sc.stop()

if __name__ == "__main__":
    main()