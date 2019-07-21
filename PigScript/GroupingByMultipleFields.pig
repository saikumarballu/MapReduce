records = LOAD 'StudentData' using PigStorage(' ') as (name:chararray,subject:chararray,score:int);
grouped_records = GROUP records BY (name,subject);
dump grouped_records;
