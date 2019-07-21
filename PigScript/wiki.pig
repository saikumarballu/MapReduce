records = LOAD 'wiki'using PigStorage(' ') as (projectName:chararray,pageName:chararray,pageCount:int,pageSize:int);
filtered_records = FILTER records by projectName == 'en';
grouped_records = GROUP filtered_records by pageName;
results = FOREACH grouped_records generate group, SUM(filtered_records.pageCount);
sorted_result = ORDER results by $1 desc;
STORE sorted_result INTO  'wikiOP';


