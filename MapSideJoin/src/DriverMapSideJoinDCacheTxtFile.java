/********************************************

*Driver

*DriverMapSideJoinDCacheTxtFile

********************************************/

 

import java.net.URI;

 


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

public class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {

 

  @Override

	public int run(String[] args) throws Exception {

	  final String NAME_NODE = "hdfs://localhost:8020/user/bigdata/";

		if (args.length != 2) {

			System.out

					.printf("Two parameters are required- <input dir> <output dir>\n");

			return -1;

		}

 

		Job job = new Job(getConf());

		Configuration conf = job.getConfiguration();

		job.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI(NAME_NODE +"dept.txt"),conf);
		//DistributedCache.addCacheFile(new URI("/user/bigdata/dept.txt"),conf);

 

		job.setJarByClass(DriverMapSideJoinDCacheTxtFile.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);

 

		job.setNumReduceTasks(0);

 

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

 

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(),

				new DriverMapSideJoinDCacheTxtFile(), args);

		System.exit(exitCode);

	}

}

