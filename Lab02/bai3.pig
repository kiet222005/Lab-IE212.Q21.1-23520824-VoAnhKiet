-- BÀI 3 – Aspect có nhiều đánh giá negative/positive nhất
SET default_parallel 1;

--------------------------------------------------
-- 1. Load cleaned_data
--------------------------------------------------
data = LOAD '/user/anhkiet/cleaned_data' USING PigStorage(';') 
AS (id:chararray, words:chararray, aspect:chararray, category:chararray, sentiment:chararray);

--------------------------------------------------
-- 2. Lọc NULL
--------------------------------------------------
data = FILTER data BY 
    (aspect IS NOT NULL) AND 
    (sentiment IS NOT NULL);

--------------------------------------------------
-- 3. Chỉ lấy aspect + sentiment
--------------------------------------------------
aspect_sentiment = FOREACH data GENERATE aspect, sentiment;

--------------------------------------------------
-- 4. Group theo aspect + sentiment
--------------------------------------------------
grp = GROUP aspect_sentiment BY (aspect, sentiment);

aspect_sent_count = FOREACH grp GENERATE
    group.aspect AS aspect,
    group.sentiment AS sentiment,
    COUNT(aspect_sentiment) AS total;

--------------------------------------------------
-- 5. Lọc từng sentiment
--------------------------------------------------
neg = FILTER aspect_sent_count BY sentiment == 'negative';
pos = FILTER aspect_sent_count BY sentiment == 'positive';

--------------------------------------------------
-- 6. Tìm max cho negative và positive
--------------------------------------------------
-- Negative nhiều nhất
neg_grp = GROUP neg ALL;
max_neg = FOREACH neg_grp {
    sorted = ORDER neg BY total DESC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1);
}

-- Positive nhiều nhất
pos_grp = GROUP pos ALL;
max_pos = FOREACH pos_grp {
    sorted = ORDER pos BY total DESC;
    top1 = LIMIT sorted 1;
    GENERATE FLATTEN(top1);
}

--------------------------------------------------
-- 7. Store kết quả
--------------------------------------------------
STORE max_neg INTO '/user/anhkiet/output/bai3_most_negative_aspect' USING PigStorage(',');
STORE max_pos INTO '/user/anhkiet/output/bai3_most_positive_aspect' USING PigStorage(',');