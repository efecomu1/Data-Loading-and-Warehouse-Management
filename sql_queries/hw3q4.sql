CREATE DATABASE star_schema;
USE star_schema;

CREATE TABLE dim_company (
    CompanyID INT AUTO_INCREMENT PRIMARY KEY,
    CompanyName VARCHAR(255),
    Sector VARCHAR(255),
    TickerSymbol VARCHAR(10)
);

CREATE TABLE dim_date (
    Date DATE PRIMARY KEY,
    Day INT,
    Month INT,
    Year INT,
    DayOfWeek VARCHAR(20)
);

CREATE TABLE dim_mobility (
    MobilityType VARCHAR(50) PRIMARY KEY,
    Description TEXT
);

CREATE TABLE fact_performance (
    Date DATE,
    CompanyID INT,
    StockPerformance FLOAT,
    SocialMentions INT,
    MobilityTrend FLOAT,
    PRIMARY KEY (Date, CompanyID),
    FOREIGN KEY (CompanyID) REFERENCES dim_company(CompanyID),
    FOREIGN KEY (Date) REFERENCES dim_date(Date)
);

SELECT * FROM fact_performance;

# High and Low stock performance
SELECT Date, CompanyID, StockPerformance, SocialMentions, MobilityTrend
FROM fact_performance
WHERE StockPerformance IS NOT NULL
ORDER BY StockPerformance DESC
LIMIT 5; # Top 5 high performing days

SELECT Date, CompanyID, StockPerformance, SocialMentions, MobilityTrend
FROM fact_performance
WHERE StockPerformance IS NOT NULL
ORDER BY StockPerformance ASC
LIMIT 5; # Top 5 low performing days

# High and Low social mentions
SELECT Date, CompanyID, StockPerformance, SocialMentions, MobilityTrend
FROM fact_performance
WHERE SocialMentions IS NOT NULL
ORDER BY SocialMentions DESC
LIMIT 5; # Top 5 days with high social mentions

SELECT Date, CompanyID, StockPerformance, SocialMentions, MobilityTrend
FROM fact_performance
WHERE SocialMentions IS NOT NULL
ORDER BY SocialMentions ASC
LIMIT 5; # Top 5 days with low social mentions

# Mobility Trend Comparison
SELECT Date, CompanyID, MobilityTrend, StockPerformance
FROM fact_performance
ORDER BY MobilityTrend DESC;





