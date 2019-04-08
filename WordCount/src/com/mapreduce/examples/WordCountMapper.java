package com.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	
	public void map(LongWritable key,Text value, Context c) throws IOException, InterruptedException
	{
		//LongWritable key = 0
		//Text value = "hi how are you"
		
		String lines = value.toString();
		
		String[] words = lines.split(" ");
		
		for(int i=0; i<words.length; i++)
		{
			/*Text key1 = new Text(words[i]);
			IntWritable value1 = new IntWritable(1);
			
			c.write(key1,value1);*/
			
			c.write(new Text(words[i]), new IntWritable(1));
		}	
		//output = (hi,1) (how,1) (are,1) (you,1)
	}
}
