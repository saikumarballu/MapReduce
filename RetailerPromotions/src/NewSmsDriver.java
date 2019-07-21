import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewSmsDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://localhost:8020/user/bigdata/";
		// get the configuration parameters and assigns a job name
		Job conf = Job.getInstance(getConf());
		DistributedCache.addCacheFile(new URI(NAME_NODE +"sms/DeliveryStatusCodes.txt"),conf.getConfiguration());
		conf.setJarByClass(NewSmsDriver.class);
		conf.setJobName("SMS Reports");

		// setting key value types for mapper and reducer outputs
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// specifying the custom reducer class
		conf.setReducerClass(SmsReducer.class);
//conf.setNumReduceTasks(0);
		// Specifying the input directories(@ runtime) and Mappers independently
		// for inputs from multiple sources
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(conf, new Path(args[0]),
				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, UserFileMapper.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(conf, new Path(args[1]),
				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, DeliveryFileMapper.class);

		// Specifying the output directory @ runtime
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		
		if (conf.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		
		
		int exitCode = ToolRunner.run(new NewSmsDriver(), args);
	    System.exit(exitCode);
	}
}