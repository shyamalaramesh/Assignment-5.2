package mapreduce.task7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Task7Mapper extends Mapper<LongWritable, Text, SalesUnit, IntWritable>{
	public static final String NA_STRING = "NA";
	public static final String ONIDA_COMPANY = "Onida";
	
	
	/**
	 * This method takes as input key and value. The input key and value are 
	 * determined by the InputFormat class defined in the job driver. Since in this 
	 * case it is a TextInputFormat, the input key type will be LongWritable(offset of 
	 * the line from the beginning of the file) and iput value type will be of Text(the 
	 * actual contents of the line). Various fields in a particular line are separated by 
	 * pipe(|). In every line first field is company name. So splitting by pipe and checking 
	 * if the first and second field are not equal to NA. Only if they are not NA. 
	 * they will be output by the mapper,thereby filtering the records in the input file.
	 *  
	 * We have to calculate the number of units sold for each company, product and size. 
	 * So the output key will be of type SalesUnit. Tha value will be 1 because each row
	 * represents 1 sales unit sold
	 *  
	 */
	public void map(LongWritable lineOffsetInFile, Text recordInput, Context context) 
			throws IOException, InterruptedException {
		
		String[] recordFields = recordInput.toString().split("\\|");
		String companyName = recordFields[0].trim();
		String productName = recordFields[1].trim();
		if(!NA_STRING.equalsIgnoreCase(companyName) && 
				!NA_STRING.equalsIgnoreCase(productName)) {
			SalesUnit salesUnit = new SalesUnit();
			int size = Integer.parseInt(recordFields[2].trim());
			salesUnit.set(companyName, productName, size);
			context.write(salesUnit, new IntWritable(1));
		}
	} 
}
