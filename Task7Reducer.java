package mapreduce.task7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Task7Reducer extends Reducer<SalesUnit, IntWritable, SalesUnit, IntWritable>{
	
	/**
	 * The mapper outputs SalesUnit as key and value as 1
	 * Now the reducer will take a particular SalesUnit 
	 * and the list of values associated with it(thus list 
	 * was aggregated based on the equals method). 
	 * It will sum up these values and
	 * output SalesUnit(toString() method) as key 
	 * and the total as the value thereby giving the total 
	 * units sold for each product name, ompany name and size. Also by 
	 * virtue of the compareTo() method, all records in a particular reducer 
	 * are sorted in descending order of the sizes 
	 */
	@Override
	protected void reduce(SalesUnit salesUnit, Iterable<IntWritable> listOfUnitsSold,
			Reducer<SalesUnit, IntWritable, SalesUnit, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		int totalOnidaUnitsSoldInAState = 0;
		for(IntWritable unitsSold : listOfUnitsSold) {
			totalOnidaUnitsSoldInAState += unitsSold.get();
		}
		context.write(salesUnit, new IntWritable(totalOnidaUnitsSoldInAState));
	}
	
}
