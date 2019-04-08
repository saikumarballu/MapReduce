package com.mapreduce.practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//Map input - Text , Text
//Map Output - Text, IntWritable

//Reducer Input - Text, IntWritable
//Reducer Output - Text, LongWritable


public class SalaryDepidDriver extends Configured {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2)
		{
			System.out.println("Usage: Please give Input and Output dir");
			System.exit(-1);
		}
		
		Job conf = new Job();
		conf.setJobName("SalaryDepidDriver");
		conf.setJarByClass(SalaryDepidDriver.class);
		
		conf.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(SalaryDepidMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setReducerClass(SalaryDepidReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		//conf.setNumReduceTasks(0);
		
		System.exit(conf.waitForCompletion(true) ? 0:-1);
		
		}

}
