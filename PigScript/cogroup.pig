--loading persons data
persons = LOAD 'PersonData' USING PigStorage(' ') AS (personId:int,personName:chararray,city:chararray);
--loading orders data
orders  = LOAD 'OrderData' USING PigStorage(' ') AS (orderId:int,orderNumber:int,personId:int);
--using COGROUP operator to create groups

result = COGROUP persons BY personId,orders by personId;
dump result;

