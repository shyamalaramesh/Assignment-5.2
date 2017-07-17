package mapreduce.task7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SalesUnit implements WritableComparable<SalesUnit> {

	private String companyName;
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	private String productName;
	private int size;
	
	public void set(String companyName, String productName, int size) {
		this.companyName = companyName;
		this.productName =productName;
		this.size = size;
	}
	
	/**
	 * Used when deserializing objects. Please note that deserialization must be 
	 * done in the same order as that of serialization 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		companyName = in.readUTF();
		productName = in.readUTF();
		size = in.readInt();
	}

	/**
	 * Used when serializing objects
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(companyName);
		out.writeUTF(productName);
		out.writeInt(size);
	}
	
	/**
	 * Used to check if two keys are equal. Keys output from the mapper 
	 * that are equal are aggregated and given to one reducer
	 *  
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SalesUnit) {
			SalesUnit otherSalesUnit = (SalesUnit) obj;
			return companyName.equals(otherSalesUnit.getCompanyName()) && 
					productName.equals(otherSalesUnit.getProductName()) && 
					size == otherSalesUnit.getSize();
		}
		return false;
	}
	
	/**
	 * hashCode() method is used by the HashPartitioner to determine which reducer to 
	 * send this WritableComparable to if this is used as the key. In this case 
	 * we want all records belongin to a particular com[any to one reducer
	 *  
	 */
	public int hashCode() {
		return companyName.hashCode();
	}
	
	/**
	 * If the equals()  method returns false, 
	 * then what should be the sort order. 
	 * If this < otherSalesUnit return negative number
	 * If this > otherSalesUnit return positive number
	 * In this case we need descending order of sizes, so 
	 * the above logic is reversed 
	 */
	@Override
	public int compareTo(SalesUnit otherSalesUnit) {
		return otherSalesUnit.getSize() - size;
	}
	
	/**
	 * What should appear as output in the file for this WritableCOmparable 
	 */
	public String toString() {
		return companyName +"\t"+productName+"\t"+size;
	}

}
