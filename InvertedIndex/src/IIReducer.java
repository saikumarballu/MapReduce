import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IIReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> docId = new HashSet<String>();
		 for(Text value:values) {
			 docId.add(value.toString());
		 }
		 System.out.println(docId.toString());
		 context.write(key, new Text( docId.toString()));  
	}
	}


