-- 1. Load
raw_data = LOAD '/hotel-review.csv' USING PigStorage(';') 
AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- bỏ header
data = FILTER raw_data BY id != 'id';

--------------------------------------------------
-- 2. lowercase
lower_data = FOREACH data GENERATE 
    id, 
    LOWER(review) AS review, 
    LOWER(aspect) AS aspect, 
    LOWER(category) AS category, 
    LOWER(sentiment) AS sentiment;

--------------------------------------------------
-- 3. tokenize
tokens = FOREACH lower_data GENERATE 
    id, 
    FLATTEN(TOKENIZE(review, ' ,.!?:;()[]')) AS word, 
    aspect, 
    category, 
    sentiment;

--------------------------------------------------
-- 4. clean word
tokens = FILTER tokens BY 
    (word IS NOT NULL) AND 
    (word != '') AND 
    (word != ' ');

--------------------------------------------------
-- 5. load stopwords
stopwords = LOAD '/stopwords.txt' USING PigStorage() AS (stopword:chararray);

joined = JOIN tokens BY word LEFT OUTER, stopwords BY stopword;

filtered_tokens = FILTER joined BY stopwords::stopword IS NULL;

cleaned_tokens = FOREACH filtered_tokens GENERATE
    tokens::id AS id,
    tokens::word AS word,
    tokens::aspect AS aspect,
    tokens::category AS category,
    tokens::sentiment AS sentiment;
--------------------------------------------------
-- 7. group lại câu
grouped = GROUP cleaned_tokens BY (id, aspect, category, sentiment);

final_result = FOREACH grouped GENERATE 
    group.id AS id, 
    BagToString(cleaned_tokens.word, ',') AS review, 
    group.aspect AS aspect, 
    group.category AS category, 
    group.sentiment AS sentiment;

--------------------------------------------------
-- 8. store
STORE final_result INTO '/user/anhkiet/cleaned_data' USING PigStorage(';');