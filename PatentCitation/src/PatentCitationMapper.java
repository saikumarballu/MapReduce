
import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class PatentCitationMapper extends Mapper<Text, Text, Text, Text> {
  @Override
  protected void map(Text key, Text value, Context context)
          throws IOException, InterruptedException {

      String[] citation = key.toString().split(",");
      context.write(new Text(citation[1]), new Text(citation[0]));
  }
}
