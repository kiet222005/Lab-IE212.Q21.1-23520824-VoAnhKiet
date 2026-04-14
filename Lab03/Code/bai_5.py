from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("Lab3_Bai5_OccupationAnalysis").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Đường dẫn thư mục (dữ liệu nguồn lấy chung từ bai1)
    input_path = "/user/anhkiet/lab3/bai1/"
    output_path = "/user/anhkiet/lab3/bai5/output_result"

    # --- BƯỚC 1: Xử lý file occupation.txt (Tuỳ chọn để hiện tên nghề nghiệp đẹp hơn) ---
    occ_rdd = sc.textFile(input_path + "occupation.txt")
    # Tạo dict: OccupationID -> OccupationName (ví dụ: '1' -> 'Programmer')
    occ_dict = occ_rdd.map(lambda line: (line.split(',')[0], line.split(',')[1])).collectAsMap()
    bcast_occ = sc.broadcast(occ_dict)

    # --- BƯỚC 2: Tạo dictionary từ users.txt: UserID -> OccupationName ---
    users_rdd = sc.textFile(input_path + "users.txt")
    # Schema users: UserID, Gender, Age, OccupationID, Zip-code
    # Ta lấy cột 0 (UserID) và cột 3 (OccupationID), sau đó tra cứu tên nghề nghiệp
    user_occ_dict = users_rdd.map(
        lambda line: (
            line.split(',')[0], 
            bcast_occ.value.get(line.split(',')[3], line.split(',')[3]) # Nếu không có ID trong dict thì giữ nguyên
        )
    ).collectAsMap()
    
    # Broadcast user_occ_dict để chia sẻ cho tất cả các worker
    bcast_user_occ = sc.broadcast(user_occ_dict)

    # --- BƯỚC 3: Xử lý ratings và gán thông tin Occupation ---
    ratings_rdd = sc.textFile(input_path + "ratings_*.txt")
    
    def map_to_occupation(line):
        parts = line.split(',')
        user_id = parts[0]
        rating = float(parts[2])
        # Tra cứu từ Broadcast Variable (UserID -> Nghề nghiệp)
        occupation = bcast_user_occ.value.get(user_id, "Unknown")
        return (occupation, (rating, 1))

    # Phát hành cặp key-value: (Occupation, (rating, 1))
    occ_ratings_mapped = ratings_rdd.map(map_to_occupation)

    # --- BƯỚC 4: Reduce để tính tổng điểm và số lượt, sau đó tính trung bình ---
    # a. Tính (Tổng điểm, Tổng lượt đánh giá)
    reduced_occ = occ_ratings_mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # b. Tính trung bình: (Tổng điểm / Tổng lượt)
    # Cấu trúc: (Occupation, (AvgRating, TotalRatings))
    avg_occ = reduced_occ.mapValues(lambda x: (x[0] / x[1], x[1]))

    # Sắp xếp kết quả theo điểm trung bình giảm dần
    sorted_avg_occ = avg_occ.sortBy(lambda x: x[1][0], ascending=False)

    # --- ĐỊNH DẠNG VÀ XUẤT KẾT QUẢ ---
    formatted_result = sorted_avg_occ.map(
        lambda x: f"Nghề nghiệp: {x[0]:<15} | Điểm TB: {x[1][0]:.2f} | Tổng số lượt đánh giá: {x[1][1]:,}"
    )

    # Lưu ra 1 file duy nhất trên HDFS
    formatted_result.coalesce(1).saveAsTextFile(output_path)

    print("\n" + "="*60)
    print(f"[THÀNH CÔNG] Đã tính trung bình rating theo nghề nghiệp!")
    print(f"File output được lưu tại HDFS: {output_path}")
    print("="*60 + "\n")

    sc.stop()

if __name__ == "__main__":
    main()