import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class LogFileReducer extends Reducer<Text,Text,Text,IntWritable>{
	
	/*
	 * 
	 */
    private IntWritable count = new IntWritable();
    
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		int wordCount = 0;
	    for (Text value:values) {
	      wordCount ++;
	    }
	    count.set(wordCount);
	    context.write(key, count);
	}
}
