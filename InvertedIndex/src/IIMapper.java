import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IIMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String fileName = null;
	protected void setup(Context context)  throws IOException, InterruptedException {
		FileSplit fs =((FileSplit)context.getInputSplit());
		fileName = fs.getPath().getName();
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                   //TODO Write the map function implementation.
	}
}
