 import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DeliveryFileMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>
{
    //variables to process delivery report
    private String cellNumber,deliveryCode,fileTag="DR~";
   
   /* map method that process DeliveryReport.txt and frames the initial key value pairs
    Key(Text) mobile number
    Value(Text)  An identifier to indicate the source of linput(using DR for the delivery report file) + Status Code*/

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split(",");
        cellNumber = splitarray[0].trim();
        deliveryCode = splitarray[1].trim();
       
        //sending the key value pair out of mapper
        context.write(new Text(cellNumber), new Text(fileTag+deliveryCode));
     }
}
