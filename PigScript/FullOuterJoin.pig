--loading persons data
persons = LOAD 'PersonData' USING PigStorage(' ') AS (personId:int,personName:chararray,city:chararray);
--loading orders data
orders  = LOAD 'OrderData' USING PigStorage(' ') AS (orderId:int,orderNumber:int,personId:int);
-- join the relations persons and order with the personId columns

result = JOIN persons BY personId FULL OUTER, orders BY personId;
dump result;

