-- Bài 5: Dựa vào từng phân loại bình luận, xác định 5 từ liên quan nhất.
SET default_parallel 1;

-- 1. LOAD
data = LOAD '/user/anhkiet/cleaned_data' USING PigStorage(';') 
AS (id:chararray, words:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- 2. FILTER NULL
data = FILTER data BY 
    (words IS NOT NULL) AND 
    (category IS NOT NULL);

-- 3. SPLIT WORD
tokens = FOREACH data GENERATE
    category,
    FLATTEN(STRSPLIT(words, ',')) AS word;

tokens = FILTER tokens BY 
    (word IS NOT NULL) AND 
    (word != '') AND 
    (word != ' ');

-- 4. COUNT
grp = GROUP tokens BY (category, word);

word_count = FOREACH grp GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(tokens) AS freq;

-- 5. TOP 5 mỗi category
grp_cat = GROUP word_count BY category;

top_words = FOREACH grp_cat {
    sorted = ORDER word_count BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

-- 6. STORE
STORE top_words INTO '/user/anhkiet/output/bai5_top_related_words' USING PigStorage(',');