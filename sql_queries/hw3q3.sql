CREATE DATABASE hw3q3;
USE hw3q3;
SET GLOBAL local_infile = 1;
show VARIABLES like 'local_infile';
show VARIABLES like 'secure_file_priv';


CREATE TABLE IF NOT EXISTS tweets (
    ID BIGINT,
    Date BIGINT,
    Text TEXT,
    Likes INT,
    Retweets INT,
    Replies INT,
    Quotes INT,
    Ticker VARCHAR(255),
    Hashtag TEXT,
    SegmentedText TEXT,
    Segmented TEXT,
    Tone VARCHAR(255),
    price FLOAT
);


LOAD DATA LOCAL INFILE 'C:/Users/efeco/Downloads/tweets_segmented_toned_priced.csv'
INTO TABLE tweets
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(ID, Date, Text, Likes, Retweets, Replies, Quotes, Ticker, Hashtag, SegmentedText, Segmented, Tone, price);

SELECT DISTINCT Ticker FROM tweets;

SELECT 
    FROM_UNIXTIME(Date, '%Y-%m') AS YearMonth,
    CASE
    # Added the unique values of 'Ticker' column, specified everything else as 'Other'
        WHEN Text REGEXP 'DE' THEN 'DE'
        WHEN Text REGEXP 'VEGI' THEN 'VEGI'
        WHEN Text REGEXP 'TSLA' THEN 'TSLA'
        WHEN Text REGEXP 'ILMN' THEN 'ILMN'
        WHEN Text REGEXP 'PFE' THEN 'PFE'
        WHEN Text REGEXP 'AAPL' THEN 'AAPL'
        ELSE 'Other'
    END AS CompanyTicker,
    COUNT(*) AS TweetCount
FROM tweets
WHERE FROM_UNIXTIME(Date, '%Y') = '2020'
GROUP BY CompanyTicker, YearMonth
ORDER BY CompanyTicker, YearMonth;
