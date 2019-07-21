hdfs dfs -mkdir /user/training/books-input

hdfs dfs -put /home/bigdata/training_materials/developer/data/Bx-BooksCorrected.csv /user/training/books-input


CREATE TABLE IF NOT EXISTS BXDataSet (ISBN STRING,BookTitle STRING,BookAuthor STRING, YearOfPublication STRING, Publisher STRING,ImageURLS STRING,ImageURLM STRING, ImageURLL STRING) 
COMMENT 'BX-Books Table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' STORED AS TEXTFILE;

LOAD DATA INPATH '/user/training/books-input/BX-BooksCorrected.csv' OVERWRITE INTO TABLE BXDataSet;

select yearofpublication, count(booktitle) from bxdataset group by yearofpublication;

CREATE TABLE IF NOT EXISTS BXSemiDataSetWithPartition (ISBN STRING,BookTitle STRING,BookAuthor STRING, Publisher STRING) COMMENT 'BX-Books Table' PARTITIONED BY(yearofpublication string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' STORED AS TEXTFILE;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=100000;

INSERT OVERWRITE TABLE BXSemiDataSetWithPartition PARTITION(yearofpublication) SELECT ISBN,booktitle,bookauthor,publisher,yearofpublication from bxdataset;





CREATE EXTERNAL TABLE location_table(
location_code STRING,
location_name STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED by ','
LINES TERMINATED by '\n'
LOCATION '/tmp/locations';
