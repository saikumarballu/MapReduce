

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
public class AnagramReducer extends Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	 
    private Text outputValue = new Text();


  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  String output = "";
	  
	  for(Text value:values) {

              output = output + value.toString() + "~";

      }

      StringTokenizer outputTokenizer = new StringTokenizer(output,"~");

      if(outputTokenizer.countTokens()>=2)

      {

              output = output.replace("~", ",");

              outputKey.set(key.toString());

              outputValue.set(output);

              context.write(outputKey, outputValue);

      }

	  }
}
