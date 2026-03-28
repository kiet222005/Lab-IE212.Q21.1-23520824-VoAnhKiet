SET default_parallel 1;

-- ==============================
-- 1. LOAD
-- ==============================
data = LOAD '/user/anhkiet/cleaned_data' USING PigStorage(';') 
AS (
    id:chararray, 
    review:chararray, 
    aspect:chararray, 
    category:chararray, 
    sentiment:chararray
);

-- ==============================
-- 2. FILTER
-- ==============================
data = FILTER data BY 
    (review IS NOT NULL) AND 
    (category IS NOT NULL) AND 
    (aspect IS NOT NULL);

-- ==============================
-- 3. SPLIT WORD
-- ==============================
tokens = FOREACH data GENERATE
    FLATTEN(TOKENIZE(review, ',')) AS word,
    category,
    aspect;

-- ==============================
-- 4. CLEAN
-- ==============================
tokens_clean = FILTER tokens BY 
    (word IS NOT NULL) AND 
    (word != '') AND 
    (word != ' ');

-- ==============================
-- 5. WORD COUNT
-- ==============================
grp_words = GROUP tokens_clean BY word;

word_count = FOREACH grp_words GENERATE
    group AS word,
    COUNT(tokens_clean) AS freq;

-- ==============================
-- 6. TOP 5
-- ==============================
grp_all = GROUP word_count ALL;

sorted_words = FOREACH grp_all {
    ordered = ORDER word_count BY freq DESC;
    GENERATE FLATTEN(ordered);
};

top5_words = LIMIT sorted_words 5;

-- ==============================
-- 7. CATEGORY COUNT
-- ==============================
grp_category = GROUP data BY category;

category_count = FOREACH grp_category GENERATE
    group AS category,
    COUNT(data) AS total; -- Đếm số lượng dòng trong mỗi Category

-- ==============================
-- 8. ASPECT COUNT
-- ==============================
grp_aspect = GROUP data BY aspect;

aspect_count = FOREACH grp_aspect GENERATE
    group AS aspect,
    COUNT(data) AS total; -- Đếm số lượng dòng trong mỗi Aspect

-- ==============================
-- 9. STORE
-- ==============================
STORE top5_words 
INTO '/user/anhkiet/output/bai2_top_words' 
USING PigStorage(',');

STORE category_count 
INTO '/user/anhkiet/output/bai2_category' 
USING PigStorage(',');  

STORE aspect_count 
INTO '/user/anhkiet/output/bai2_aspect' 
USING PigStorage(',');