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

public class SmsReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
      
       //Variables to aid the join process
       private String customerName,deliveryReport;
       /*Map to store Delivery Codes and Messages
       Key being the status code and vale being the status message*/
       private static Map<String,String> DeliveryCodesMap= new HashMap<String,String>();
       private Path[] localFiles;
       protected void setUp(Context context)throws java.io.IOException, InterruptedException {
    	   
    		   localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    		   
    		   System.out.println("-----------localFiles from setup------------------------"+localFiles[9]);
    		  // localFiles = context.getLocalCacheFiles();
        	  // FileInputStream fileStream = new FileInputStream(cacheFiles[0].toString());
    		       
    	   
    	   
    	   loadDeliveryStatusCodes();
       }
       public void configure(Job job)
       {
              //To load the Delivery Codes and Messages into a hash map
    	   System.out.println("-----------localFiles from configure------------------------"+localFiles);
    	   try{	
    		   localFiles = DistributedCache.getLocalCacheFiles(job.getConfiguration());
    		     }catch(Exception e){ 
    		   e.printStackTrace();
    		   }            
    	   loadDeliveryStatusCodes();
             
       }


       public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	  // localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	  // loadDeliveryStatusCodes();
		   //System.out.println("-----------localFiles from setup------------------------"+localFiles[9]); 
        while (values.iterator().hasNext())
        {
             String currValue = values.iterator().next().toString();
             String valueSplitted[] = currValue.split("~");
             /*identifying the record source that corresponds to a cell number
             and parses the values accordingly*/
             if(valueSplitted[0].equals("CD"))
             {
               customerName=valueSplitted[1].trim();
             }
             else if(valueSplitted[0].equals("DR"))
             {
              //getting the delivery code and using the same to obtain the Message
               deliveryReport = DeliveryCodesMap.get(valueSplitted[1].trim());
             }
        }
        
        //pump final output to file
        if(customerName!=null && deliveryReport!=null)
        {
               context.write(new Text(customerName), new Text(deliveryReport));
        }
        else if(customerName==null)
        	context.write(new Text("customerName"), new Text(deliveryReport));
        else if(deliveryReport==null)
        	context.write(new Text(customerName), new Text("deliveryReport"));
        
    }
      
      
       //To load the Delivery Codes and Messages into a hash map
    private void loadDeliveryStatusCodes()
    {
       String strRead;
       try {
              //read file from Distributed Cache
    	   BufferedReader reader = new BufferedReader(new FileReader(localFiles[0].toString()));
                     //BufferedReader reader = new BufferedReader(new FileReader("DeliveryStatusCodes.txt"));
                     while ((strRead=reader.readLine() ) != null)
                     {
                           String splitarray[] = strRead.split(",");
                           //parse record and load into HahMap
                           DeliveryCodesMap.put(splitarray[0].trim(), splitarray[1].trim());
                          
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
		 loadDeliveryStatusCodes();
	}
}