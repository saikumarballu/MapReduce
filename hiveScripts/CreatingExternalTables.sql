hdfs dfs -mkdir /locations

hdfs dfs -put /home/bigdata/training_materials/developer/data/employees-data/us_states.csv /locations


CREATE EXTERNAL TABLE location_table(
location_code STRING,
location_name STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED by ','
LINES TERMINATED by '\n'
LOCATION '/locations';

SELECT * FROM location_table;

DROP TABLE location_table;
