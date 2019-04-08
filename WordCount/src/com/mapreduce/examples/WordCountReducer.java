package com.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,LongWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context c) throws IOException, InterruptedException
	{
		long sum = 0;
		//hi [1,1,1,1,1,1]
		while(values.iterator().hasNext())
		{
			IntWritable iw = values.iterator().next();
			sum = sum + iw.get();
		}
		c.write(key, new LongWritable(sum));
	}
}
