package org.learning.hadoop.sampleapp.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReader {

	public static void main(String[] args) throws IOException {
		Path path = new Path("/user/bigdata/sample-data.seq");
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			System.out.println("key : " + key + " - value : " + new String(value.getBytes()));
		}
		IOUtils.closeStream(reader);
	}
}
