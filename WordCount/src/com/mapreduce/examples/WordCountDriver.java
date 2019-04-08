package com.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver extends Configured{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length < 2)
		{
			System.out.println("Please provide the input file path and out directory...");
			System.exit(-1);
		}
		
		Job conf = new Job();
		conf.setJarByClass(WordCountDriver.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(WordCountMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setReducerClass(WordCountReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
	/*	boolean flag = conf.waitForCompletion(true);
		
		if(flag)
		{
			System.exit(0);
		}
		else
		{
			System.exit(-1);
		}*/
		System.exit(conf.waitForCompletion(true) ? 0:-1);
			
	}

}
