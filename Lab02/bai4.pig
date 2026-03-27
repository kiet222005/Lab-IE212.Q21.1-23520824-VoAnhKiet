SET default_parallel 1;

-- 1. LOAD
data = LOAD '/user/anhkiet/cleaned_data' USING PigStorage(';') 
AS (id:chararray, words:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- 2. FILTER NULL
data = FILTER data BY 
    (words IS NOT NULL) AND 
    (category IS NOT NULL) AND 
    (sentiment IS NOT NULL);

-- 3. SPLIT WORD
tokens = FOREACH data GENERATE
    category,
    sentiment,
    FLATTEN(STRSPLIT(words, ',')) AS word;

tokens = FILTER tokens BY 
    (word IS NOT NULL) AND 
    (word != '') AND 
    (word != ' ');

-- ==============================
-- POSITIVE
-- ==============================
pos_tokens = FILTER tokens BY sentiment == 'positive';

grp_pos = GROUP pos_tokens BY (category, word);

pos_count = FOREACH grp_pos GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(pos_tokens) AS freq;

grp_pos_cat = GROUP pos_count BY category;

top_pos = FOREACH grp_pos_cat {
    sorted = ORDER pos_count BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

-- ==============================
-- NEGATIVE
-- ==============================
neg_tokens = FILTER tokens BY sentiment == 'negative';

grp_neg = GROUP neg_tokens BY (category, word);

neg_count = FOREACH grp_neg GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(neg_tokens) AS freq;

grp_neg_cat = GROUP neg_count BY category;

top_neg = FOREACH grp_neg_cat {
    sorted = ORDER neg_count BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

-- ==============================
-- STORE
-- ==============================
STORE top_pos INTO '/user/anhkiet/output/bai4_top_positive_words' USING PigStorage(',');
STORE top_neg INTO '/user/anhkiet/output/bai4_top_negative_words' USING PigStorage(',');