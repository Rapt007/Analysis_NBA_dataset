DROP TABLE Matrix_first;
DROP TABLE Matrix_second;
DROP TABLE RESULT_MULTIPLY;

CREATE TABLE Matrix_first(
i int, j int , value double
)
COMMENT 'Matrix_first details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' Stored as TEXTFILE;

CREATE TABLE Matrix_second(
i int, j int , value double
)
COMMENT 'Matrix_second details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as TEXTFILE;

LOAD DATA LOCAL INPATH '${hiveconf:M}' OVERWRITE INTO TABLE Matrix_first;
LOAD DATA LOCAL INPATH '${hiveconf:N}' OVERWRITE INTO TABLE Matrix_second;

CREATE TABLE RESULT_MULTIPLY AS SELECT m1.i as i,m2.j as j, (m1.value*m2.value) as value from Matrix_first AS m1 JOIN Matrix_second AS m2 ON (m1.j=m2.i);
SELECT i,j, sum(value) FROM RESULT_MULTIPLY GROUP BY i,j; 




