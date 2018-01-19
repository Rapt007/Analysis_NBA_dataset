-- loading the matrix M and separating it through comma and creating a schema 
matrix_first = LOAD '$M' USING PigStorage(',') AS (row, col, value);

-- loading the matrix N and separating it through comma and creating a schema
matrix_second = LOAD '$N' USING PigStorage(',') AS (row, col, value);

-- join the matrices on columns of 1st matrix and rows of 2nd matrix
result = JOIN matrix_first BY col, matrix_second BY row;

-- for each row generate the rows from the 1st matrix and cols from 2nd matrix and result of multiplication of value of 1st matrix and value from 2nd matrix 
results = FOREACH result GENERATE $0 AS row ,$4 AS col ,$2*$5 AS value ;

--grouping the result_after_multiply based on same (rows from 1st matrix, cols from 2 nd matrix) pair 
result_after_grouping = GROUP results  BY (row, col);
-- for each row  in result_after_grouping getting the pair of row and cols and  the values which is 3 column in the bag of tuples 
adding_res = FOREACH result_after_grouping GENERATE group.$0 as row , group.$1 as col, SUM(results.value) AS val ;

--printing new_val on the screen 
DUMP adding_res;
STORE adding_res INTO '$O' USING PigStorage(',');
