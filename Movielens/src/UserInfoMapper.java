 import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserInfoMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>
{
    //variables to process Consumer Details
    private String userID,age,fileTag="UI~";
   
    /* map method that process ConsumerDetails.txt and frames the initial key value pairs
       Key(Text)  mobile number
       Value(Text)  An identifier to indicate the source of input(using CD for the customer details file) + Customer Name
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split("::");
        userID = splitarray[0].trim();
        age = splitarray[2].trim();
       
      //sending the key value pair out of mapper
        context.write(new Text(userID), new Text(fileTag+age));
     }
}
