
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ComputeKey implements WritableComparable<ComputeKey>{
	private Text computekey = new Text();
	private IntWritable Flag = new IntWritable();
	public static final IntWritable RECT_RECORD = new IntWritable(0);
	public static final IntWritable POINT_RECORD = new IntWritable(1);
	public Text getComputeKey(){
		return ComputeKey;
	}
	public IntWritable getFlag(){
		return Flag;
	}
	public ComputeKey(){}
	public ComputeKey(String key, int order){
		computekey.set(key);
		Flag.set(order);
	}
	@Override
	public int compareTo(ComputeKey gkey) {
		int compareVal = this.computekey.compareTo(gkey.getComputeKey());
		if (compareVal == 0) compareVal = this.Flag.compareTo(gkey.getFlag());
		return compareVal;
	}
	@Override
	public void readFields(DataInput In) throws IOException {
		computekey.readFields(In);
		Flag.readFields(In);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		computekey.write(out);
		Flag.write(out);
	}
	public boolean equals (ComputeKey other) {
		return this.computekey.equals(other.ComputeKey()) && this.Flag.equals(other.getFlag() );
	}
	public int hashCode() {
		return this.computekey.hashCode();
	}
}