package mapreduce.task6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Task6Driver {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		// initialize the configuration
		Configuration conf = new Configuration();
		
		// create a job object from the configuration and give it any name you want 
		Job job = new Job(conf, "Assignment_5.2 -> Task_6 -> "
				+ "Number_Of_Onida_Units_Sold_Per_State");
		
		// java.lang.Class object of the driver class
		job.setJarByClass(Task6Driver.class);

		// map function outputs key-value pairs. 
		// What is the type of the key in the output 
		job.setMapOutputKeyClass(Text.class);
		// map function outputs key-value pairs. 
		// What is the type of the value in the output
		job.setMapOutputValueClass(IntWritable.class);
		
		// reduce function outputs key-value pairs. 
		// What is the type of the key in the output. 
		// In this case output is number of Onida units sold per state. 
		// So key type is Text 
		job.setOutputKeyClass(Text.class);
		// reduce function outputs key-value pairs. 
		// What is the type of the value in the output. 
		// In this case output is number of Onida units sold per state. 
		// So value type is IntWritable 
		job.setOutputValueClass(IntWritable.class);
		
		// Mapper class which has implemenation for the map phase
		job.setMapperClass(Task6Mapper.class);
		
		// Reducer class which has implemenation for the reduce phase
		job.setReducerClass(Task6Reducer.class);
		
		// Combiner class which is used to aggregate some keys in the map phase itself
		// this reduces the number of key value paird to be transferred across 
		// the network in the shuffle and sort phase thus saving bandwidth
		job.setCombinerClass(Task6Reducer.class);
		
		// Input is a text file. So input format is TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);
		
		// Output is also a text file. So output format is TextOutputFormat
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * The input path to the job. The map task will
		 * read the files in this path form HDFS 
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/*
		 * The output path from the job. The map/reduce task will
		 * write the files to this path to HDFS. In this case the 
		 * reduce task will write to output path because number of 
		 * reducer tasks is not explicitly configured to be zero  
		 */
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
