LOAD DATA
INFILE 'aisles.csv'
LOGFILE 'tibero_aisles.log'
BADFILE 'tibero_aisles.bad'
APPEND
INTO TABLE aisles
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
(aisle_id, aisle)
