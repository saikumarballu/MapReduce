import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;



public class MRUnitTests {

	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver = null;
	MapDriver<LongWritable,Text,Text,IntWritable> mapDriver = null;
	MapReduceDriver<LongWritable,Text,Text,IntWritable,Text,IntWritable> mapreduceDriver = null;
	
	@Before
	public void setup() {
		mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>();
		reduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		mapreduceDriver = new MapReduceDriver<LongWritable,Text,Text,IntWritable,Text,IntWritable>();
		WordMapper mapper = new WordMapper();
		SumReducer reducer = new SumReducer();
		mapDriver.setMapper(mapper);
		reduceDriver.setReducer(reducer);
		mapreduceDriver.setMapper(mapper);
		mapreduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.setInput(new LongWritable(), new Text("cat cat dog"));
		mapDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapDriver.withOutput(new Text("dog"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(20));
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		reduceDriver.withInput(new Text("cat"), values);
		reduceDriver.withOutput(new Text("cat"), new IntWritable(23));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducer() throws IOException {
		mapreduceDriver.withInput(new LongWritable(), new Text("cat cat dog dog dog"));
		mapreduceDriver.addOutput(new Text("cat"), new IntWritable(2));
		mapreduceDriver.addOutput(new Text("dog"), new IntWritable(3));
		mapreduceDriver.runTest();
	}
}