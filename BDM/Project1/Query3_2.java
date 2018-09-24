import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;


import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Customer{

  public static class MapClass extends MapReduceBase
       implements Mapper<LongWritable, Text, Text, Text>{

	private Text customerId= new Text();
	private Text transactionTotal=new Text();
//	private IntWritable transactionTotal= new IntWritable();
//	private FloatWritable salary= new FloatWritable();

    @Override
    public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException{

	String[] row=value.toString().split(",");
	int transactionId=Integer.parseInt(row[0]);
	String customerId1=row[1];
	String transactionTotal1=row[2];
//	int transactionTotal1=Integer.parseInt(row[2]);
	int transactionNumberItems=Integer.parseInt(row[3]);
	String transactionDescription=row[4];

	customerId.set(customerId1);
	transactionTotal.set(1+","+transactionTotal1);
	output.collect(customerId,transactionTotal);
	}

    }
  

  public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
    public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
	int countTotal=0,count=0;
	String transactionTotal="", counts="";
	while(values.hasNext()){
		String tokens[]=(values.next().toString()).split(",");
		count=Integer.parseInt(tokens[0]);
		transactionTotal=tokens[1];
		countTotal=countTotal+count;
		
		counts=Integer.toString(countTotal);

//count=Integer.parseInt(tokens[0]);
//		transactionTotal=Integer.parseInt(tokens[1]);
	}
	output.collect(key, new Text(counts+","+transactionTotal));
    }
  }


   public static void main(String[] args){
	JobClient my_client=new JobClient();
	JobConf job_conf=new JobConf(Customer.class);

	job_conf.setJobName("CustomersInCountryCode2to6");
	job_conf.setOutputKeyClass(Text.class);
	job_conf.setOutputValueClass(Text.class);

	job_conf.setMapperClass(Customer.MapClass.class);
	job_conf.setCombinerClass(Customer.ReduceClass.class);
	job_conf.setReducerClass(Customer.ReduceClass.class);	

	job_conf.setInputFormat(TextInputFormat.class);
	job_conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

	my_client.setConf(job_conf);

	try{
		JobClient.runJob(job_conf);
	}
	catch(Exception e){
		e.printStackTrace();
	}
  	}
}