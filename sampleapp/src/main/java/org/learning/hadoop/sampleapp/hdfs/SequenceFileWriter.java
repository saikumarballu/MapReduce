package org.learning.hadoop.sampleapp.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileWriter {
	public static void main(String args[]) throws IOException {
		File dataFile = new File("/home/bigdata/sample.txt");
		String sequenceFileName = "/user/bigdata/sample-data.seq";
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));

		IntWritable key = null;
		BytesWritable value = null;
		Path path = new Path(sequenceFileName);

		if ((conf != null) && (dataFile != null) && (dataFile.exists())) {
			SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
					SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD),
					SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(BytesWritable.class));

			List<String> lines = FileUtils.readLines(dataFile);

			for (int i = 0; i < lines.size(); i++) {
				value = new BytesWritable(lines.get(i).getBytes());
				key = new IntWritable(i);
				writer.append(key, value);
			}
			IOUtils.closeStream(writer);
		}
	}
}