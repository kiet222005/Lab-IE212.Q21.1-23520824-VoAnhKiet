from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("Lab3_Bai2_GenreAnalysis").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn file đầu vào (lấy từ bai1) và đầu ra (lưu ở bai2)
    input_path = "/user/anhkiet/lab3/bai1/"
    output_path = "/user/anhkiet/lab3/bai2/output_result"

    # --- BƯỚC 1: Tạo map (MovieID -> List of Genres) ---
    movies_rdd = sc.textFile(input_path + "movies.txt")
    
    def extract_genres(line):
        parts = line.split(',', 1) # Tách ID
        movie_id = parts[0]
        # Lấy cụm thể loại ở cuối dòng
        genres_str = line.rsplit(',', 1)[1] 
        genres_list = genres_str.split('|')
        return (movie_id, genres_list)

    movie_genre_dict = movies_rdd.map(extract_genres).collectAsMap()
    
    # Broadcast dict này tới các worker
    bcast_genres = sc.broadcast(movie_genre_dict)


    # --- BƯỚC 2: Map từ MovieID -> Rating -> (Genre, Rating) ---
    ratings_rdd = sc.textFile(input_path + "ratings_*.txt")
    
    # Hàm này nhận vào 1 dòng rating, trả về một LIST các tuples: [(Genre1, (Rating, 1)), (Genre2, (Rating, 1)), ...]
    def map_rating_to_genres(line):
        parts = line.split(',')
        movie_id = parts[1]
        rating = float(parts[2])
        
        # Lấy danh sách thể loại của phim này từ biến broadcast
        genres = bcast_genres.value.get(movie_id, ["Unknown"])
        
        # Trả về list để dùng với flatMap
        return [(genre, (rating, 1)) for genre in genres]

    # Sử dụng flatMap để "duỗi" list ra, biến 1 dòng rating thành nhiều dòng (mỗi dòng 1 thể loại)
    genre_ratings_rdd = ratings_rdd.flatMap(map_rating_to_genres)


    # --- BƯỚC 3: Tính trung bình điểm đánh giá cho từng thể loại ---
    # a. ReduceByKey để tính Tổng điểm và Tổng lượt đánh giá cho từng thể loại
    reduced_genres = genre_ratings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # b. Tính điểm trung bình: (Tổng điểm / Tổng lượt)
    # Kết quả: (Genre, (AvgRating, TotalRatings))
    avg_genres = reduced_genres.mapValues(lambda x: (x[0] / x[1], x[1]))

    sorted_avg_genres = avg_genres.sortBy(lambda x: x[1][0], ascending=False)

    # --- ĐỊNH DẠNG VÀ XUẤT KẾT QUẢ RA HDFS ---
    # Format text thành dạng: Thể loại, Điểm trung bình, Tổng lượt đánh giá
    formatted_result = sorted_avg_genres.map(
        lambda x: f"Thể loại: {x[0]:<15} | Điểm TB: {x[1][0]:.2f} | Tổng lượt rating: {x[1][1]}"
    )

    # Lưu ra file (dùng coalesce(1) để gộp thành 1 file part-00000 duy nhất)
    formatted_result.coalesce(1).saveAsTextFile(output_path)

    print("\n" + "="*60)
    print(f"[THÀNH CÔNG] Phân tích thể loại hoàn tất!")
    print(f"Đã xuất kết quả ra HDFS tại: {output_path}")
    print("="*60 + "\n")

    sc.stop()

if __name__ == "__main__":
    main()