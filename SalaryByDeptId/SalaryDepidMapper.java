package com.mapreduce.practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryDepidMapper extends Mapper<Text, Text, Text, IntWritable> 
{
	public void map(Text key, Text value, Context con) throws IOException, InterruptedException 
	{
		String lines = value.toString();
		String[] words = lines.split("\t");
		//System.out.println(words);
		int value1 = Integer.parseInt(words[1]);
		Text key1 = new Text(words[2]);
		con.write(key1, new IntWritable(value1));

	}
}