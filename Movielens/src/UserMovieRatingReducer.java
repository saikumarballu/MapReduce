 import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class UserMovieRatingReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
      
       //Variables to aid the join process
       private String age,movieName,rating;
       /*Map to store Delivery Codes and Messages
       Key being the status code and vale being the status message*/
       private static Map<String,String> moviesMap= new HashMap<String,String>();
       private Path[] localFiles;
             


       public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	 String value="";
        while (values.iterator().hasNext())
        {
             String currValue = values.iterator().next().toString();
             String valueSplitted[] = currValue.split("~");
             /*identifying the record source that corresponds to a cell number
             and parses the values accordingly*/
            
             if(valueSplitted[0].equals("UI"))
             {
               age=valueSplitted[1].trim();
               
             }
             else if(valueSplitted[0].equals("UR"))
             {
              //getting the delivery code and using the same to obtain the Message
               movieName = moviesMap.get(valueSplitted[1].trim());
               rating=valueSplitted[2].trim();
               value+=movieName+"("+rating+"),";
             }
             
        }
        
        //pump final output to file
        
               context.write(key, new Text(age+","+value));
        
        
        
    }
      
      
       //To load the Delivery Codes and Messages into a hash map
    private void loadAllMovies()
    {
       String strRead;
       try {
              //read file from Distributed Cache
    	   BufferedReader reader = new BufferedReader(new FileReader(localFiles[0].toString()));
                     while ((strRead=reader.readLine() ) != null)
                     {
                           String splitarray[] = strRead.split("::");
                           //parse record and load into HahMap
                           moviesMap.put(splitarray[0].trim(), splitarray[1].trim());//+"|"+splitarray[2].trim());
                          
                     }
              }
              catch (FileNotFoundException e) {
              e.printStackTrace();
              }catch( IOException e ) {
                       e.printStackTrace();
                }
             
       }
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		 loadAllMovies();
	}
}