import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogAnalyzer extends Configured implements Tool {

	public static void main(String[] args) throws Exception
	{
		 
			ToolRunner.run(new LogAnalyzer(), args);
	}

	@Override
	public int run(String[] args) throws Exception
	{
	
		Job job =  Job.getInstance(getConf());
		job.setJobName("LogAnalyzer Job");
		job.setJarByClass(LogAnalyzer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(LogFileMapper.class);
		job.setReducerClass(LogFileReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(12);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

}
