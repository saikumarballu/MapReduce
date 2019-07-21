--loading the student data
students = load 'StudentData' using PigStorage(' ') as (name:chararray,subject:chararray,score:int);
--grouping by subject
students_per_subject = GROUP students BY subject;
--dumping the results
dump students_per_subject

