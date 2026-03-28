-- ==============================================================
-- CÂU 1: TIỀN XỬ LÝ DỮ LIỆU HOTEL REVIEWS
-- ==============================================================

-- 0. ĐĂNG KÝ THƯ VIỆN PIGGYBANK (Để dùng CSVExcelStorage)
-- Hãy đảm bảo đường dẫn này chính xác với máy của bạn
REGISTER /home/anhkiet/pig/lib/piggybank.jar;

SET default_parallel 1;

-- ==============================
-- 1. LOAD DATA 
-- ==============================
-- Sử dụng CSVExcelStorage để không bị tách nhầm cột khi gặp dấu ; bên trong ngoặc kép
raw_data = LOAD '/hotel-review.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'YES_MULTILINE', 'UNIX') 
AS (
    id:chararray, 
    review:chararray, 
    aspect:chararray, 
    category:chararray, 
    sentiment:chararray
);

-- ==============================
-- 2. REMOVE HEADER + NULL
-- ==============================
data = FILTER raw_data BY 
    (id IS NOT NULL) AND
    (review IS NOT NULL) AND
    (id != 'id');

-- ==============================
-- 3. CLEAN REVIEW (CHUYỂN DẤU CÂU THÀNH KHOẢNG TRẮNG)
-- ==============================
-- Bước này cực kỳ quan trọng để khi TOKENIZE theo khoảng trắng, từ sẽ sạch sẽ
clean_data = FOREACH data GENERATE
    id,
    TRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(review, '""', ' '), 
                    '"', ' '), 
                '[,.!?:;()\\[\\]+\\-*/]', ' '),    -- THÊM DẤU ; VÀO ĐÂY ĐỂ NÓ XÓA SẠCH
            '\\n', ' '), 
        '\\r', ' ')
    ) AS review,
    aspect,
    category,
    sentiment;

-- ==============================
-- 4. LOWERCASE
-- ==============================
lower_data = FOREACH clean_data GENERATE 
    id, 
    LOWER(review) AS review, 
    aspect, 
    category, 
    sentiment;

-- ==============================
-- 5. TOKENIZE (TÁCH THEO KHOẢNG TRẮNG)
-- ==============================
-- Làm đúng yêu cầu đề bài: Tách theo khoảng trắng
tokens = FOREACH lower_data GENERATE 
    id, 
    FLATTEN(TOKENIZE(review, ' ')) AS word, 
    aspect, 
    category, 
    sentiment;

-- ==============================
-- 6. CLEAN WORD
-- ==============================
-- Loại bỏ các từ rỗng phát sinh trong quá trình tokenize
tokens = FILTER tokens BY 
    (word IS NOT NULL) AND 
    (word != '') AND 
    (word != ' ');

-- ==============================
-- 7. REMOVE STOPWORDS
-- ==============================
stopwords = LOAD '/stopwords.txt' USING PigStorage() 
AS (stopword:chararray);

-- Join để lọc bỏ stopword
joined = JOIN tokens BY word LEFT OUTER, stopwords BY stopword;

filtered = FILTER joined BY stopwords::stopword IS NULL;

cleaned_tokens = FOREACH filtered GENERATE
    tokens::id AS id,
    tokens::word AS word,
    tokens::aspect AS aspect,
    tokens::category AS category,
    tokens::sentiment AS sentiment;

-- ==============================
-- 8. GROUP LẠI THÀNH CÂU ĐÃ CLEAN
-- ==============================
grouped = GROUP cleaned_tokens BY (id, aspect, category, sentiment);

final_result = FOREACH grouped GENERATE 
    group.id AS id, 
    BagToString(cleaned_tokens.word, ',') AS review, 
    group.aspect AS aspect, 
    group.category AS category, 
    group.sentiment AS sentiment;

-- ==============================
-- 9. STORE
-- ==============================
-- Lưu kết quả cuối cùng vào HDFS
STORE final_result 
INTO '/user/anhkiet/cleaned_data' 
USING PigStorage(';');