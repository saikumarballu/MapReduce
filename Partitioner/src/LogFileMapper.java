
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LogFileMapper extends Mapper<LongWritable,Text,Text,Text>{

	private Text ipKey = new Text();
	private Text month = new Text();

	/*
	 * Input line looks like: 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400]
	 * "GET /cat.jpg HTTP/1.1" 200 12433
	 */
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String[] fields = value.toString().split(" ");
		if (fields.length > 3) {
			String ip = fields[0];
			String[] dtFields = fields[3].split("/");
			if (dtFields.length > 1) {
				String theMonth = dtFields[1];
				ipKey.set(ip);
				month.set(theMonth);
				context.write(ipKey, month);
			}
		}
	}

}
