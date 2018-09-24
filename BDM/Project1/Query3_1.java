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

public class Query3_1{

  public static class MapClass extends MapReduceBase
       implements Mapper<LongWritable, Text, Text, IntWritable>{

	private Text name= new Text();
	private IntWritable countryCode= new IntWritable();	
//	private FloatWritable salary= new FloatWritable();

    @Override
    public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException{

	String[] row=value.toString().split(",");
	int id=Integer.parseInt(row[0]);
	String name1=row[1];
	int age=Integer.parseInt(row[2]);
	int countryCode1=Integer.parseInt(row[3]);
	float salary=Float.parseFloat(row[4]);

	if (countryCode1>=2 && countryCode1<=6)
	{
		name.set(name1);
		countryCode.set(countryCode1);
		output.collect(new Text(name1),countryCode);
	}

    }
  }
/*
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
    public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
*/

   public static void main(String[] args){
	JobClient my_client=new JobClient();
	JobConf job_conf=new JobConf(Customer.class);

	job_conf.setJobName("CustomersInCountryCode2to6");
	job_conf.setOutputKeyClass(Text.class);
	job_conf.setOutputValueClass(IntWritable.class);

	job_conf.setMapperClass(Customer.MapClass.class);

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