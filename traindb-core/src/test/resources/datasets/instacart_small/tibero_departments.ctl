LOAD DATA
INFILE 'departments.csv'
LOGFILE 'tibero_departments.log'
BADFILE 'tibero_departments.bad'
APPEND
INTO TABLE departments
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
(department_id, department)
