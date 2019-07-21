import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserMovieRatingsDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		final String NAME_NODE = "hdfs://localhost:8020/user/bigdata/";
		// get the configuration parameters and assigns a job name
		Job conf = Job.getInstance(getConf());
		DistributedCache.addCacheFile(new URI(NAME_NODE +"movies/movies.dat"),conf.getConfiguration());
		conf.setJarByClass(UserMovieRatingsDriver.class);
		conf.setJobName("Movie Ratings");

		// setting key value types for mapper and reducer outputs
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// specifying the custom reducer class
		conf.setReducerClass(UserMovieRatingReducer.class);
//conf.setNumReduceTasks(0);
		// Specifying the input directories(@ runtime) and Mappers independently
		// for inputs from multiple sources
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(conf, new Path(args[0]),
				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, UserInfoMapper.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(conf, new Path(args[1]),
				org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, MovieRatingsMapper.class);

		// Specifying the output directory @ runtime
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		
		if (conf.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		
		
		int exitCode = ToolRunner.run(new UserMovieRatingsDriver(), args);
	    System.exit(exitCode);
	}
}