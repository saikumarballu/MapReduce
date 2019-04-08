package com.mapreduce.practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryDepidReducer extends Reducer <Text,IntWritable,Text,LongWritable> {
	
	public void reduce(Text key,Iterable<IntWritable> values, Context c) throws IOException, InterruptedException
	{
		int sum = 0;
		
		while(values.iterator().hasNext())
		{
			IntWritable iw = values.iterator().next();
			sum = sum + iw.get();
		}
		c.write(key, new LongWritable(sum));

		
	}

}
