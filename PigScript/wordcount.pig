rawdata = load 'shakespeare' using PigStorage() as (c:chararray);
B = foreach rawdata generate flatten(TOKENIZE(c)) as word;
C = filter B by word matches '\\w+';
D = group C by word;
F = foreach D generate group,COUNT(C);
store E into 'wordcount_pig_op';
