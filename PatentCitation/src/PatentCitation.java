
import java.io.File;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PatentCitation extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf(
          "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
              .getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    
    Path inputPath = new Path(args[0]);
    File outputDir = new File(args[1]);
    //deleteFilesInDirectory(outputDir);
    Path outputPath = new Path(args[1]);

    Job job = new Job(getConf(), "Hadoop Patent Citation Example");
    job.setJarByClass(PatentCitation.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(PatentCitationMapper.class);
    job.setReducerClass(PatentCitationReducer.class); 

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setNumReduceTasks(1);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
  private  void deleteFilesInDirectory(File f) throws Exception {
      if (f.isDirectory()) {
          for (File c : f.listFiles())
              deleteFilesInDirectory(c);
      }
      if (!f.delete())
          throw new FileNotFoundException("Failed to delete file: " + f);
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PatentCitation(), args);
    System.exit(exitCode);
  }
}
