package mapreduce.task6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Task6Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	/**
	 * The mapper outputs stateName as key and value as 1. The combiner will take 
	 * this output and aggregate the keys 
	 * Now the reducer will take a particular stateName 
	 * and the list of values associated with it. It will sum up these values and
	 * output stateName as key and the total as the value thereby giving the total 
	 * units sold in each state for Onida
	 */
	@Override
	protected void reduce(Text stateName, Iterable<IntWritable> listOfUnitsSold,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		int totalOnidaUnitsSoldInAState = 0;
		for(IntWritable unitsSold : listOfUnitsSold) {
			totalOnidaUnitsSoldInAState += unitsSold.get();
		}
		context.write(stateName, new IntWritable(totalOnidaUnitsSoldInAState));
	}
	
}
