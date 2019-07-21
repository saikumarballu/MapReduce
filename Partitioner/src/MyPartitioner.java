import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class MyPartitioner extends Partitioner<Text,Text>{

	Map<String,Integer> months = new HashMap<String,Integer>();

	public MyPartitioner() {
		months.put("Jan", 0);
		months.put("Feb", 1);
		months.put("Mar", 2);
		months.put("Apr", 3);
		months.put("May", 4);
		months.put("Jun", 5);
		months.put("Jul", 6);
		months.put("Aug", 7);
		months.put("Sep", 8);
		months.put("Oct", 9);
		months.put("Nov", 10);
		months.put("Dec", 11);
	}

	public int getPartition(Text key, Text value,
			int numReduceTasks) {

		return (months.get(value.toString()));
	}

}
