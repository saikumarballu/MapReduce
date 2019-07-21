records = LOAD 'PigSampleData' using PigStorage(' ') as (c1:chararray,c2:int,c3:int);
grouped_records = GROUP records BY c2 * c3;
dump grouped_records;
