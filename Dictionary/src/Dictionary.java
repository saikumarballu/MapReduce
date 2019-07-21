
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Dictionary extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
              .getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    
    Configuration conf = new Configuration();





    Job job = new Job(conf, "dictionary");





    job.setJarByClass(Dictionary.class);





    job.setMapperClass(DictionaryMapper.class);





    job.setReducerClass(DictionaryReducer.class);





    job.setOutputKeyClass(Text.class);





    job.setOutputValueClass(Text.class);





    job.setInputFormatClass(KeyValueTextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path("output"));



    if (job.waitForCompletion(true)) {
        return 0;
      }
      return 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Dictionary(), args);
    System.exit(exitCode);
  }
}
