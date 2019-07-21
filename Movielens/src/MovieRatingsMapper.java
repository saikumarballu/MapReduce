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

public class MovieRatingsMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>
{
    //variables to process User Ratings
    private String userID,movieID,rating,fileTag="UR~";
   
   /* map method that process ratings.dat and frames the initial key value pairs
    Key(Text) UserID
    Value(Text)  An identifier to indicate the source of input(using UR for the User Rating  file) + movie id + rating*/

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        System.out.println(line);
        String splitarray[] = line.split("::");
        userID = splitarray[0].trim();
        movieID = splitarray[1].trim();
        rating = splitarray[2].trim();
       
        //sending the key value pair out of mapper
        context.write(new Text(userID), new Text(fileTag+movieID+"~"+rating));
     }
}
