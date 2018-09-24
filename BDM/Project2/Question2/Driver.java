import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
public static class MapperClass extends Mapper<LongWritable, Text, IntWritable, Text>
{
	private IntWritable Outkey = new IntWritable();
	private Text val = new Text();

	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
	{
		String[] line = value.toString().split(",");
		int flag = Integer.parseInt(line[5]);
		Outkey.set(flag);
		String one= "1";
		val.set(one);
		context.write(Outkey, val);
	}
}
public static class ReducerClass extends Reducer<IntWritable, Text, IntWritable, Text>
{
	public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException
	{
		int count = 0;
		for (Text t : values)
		{
			count++;
		}
		String res=Integer.toString(count);
		context.write(key, new Text(res));
	}
}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration(true);
		Job job = new Job(conf, "Json Input");

		job.setJarByClass(Driver.class);
		job.setInputFormatClass(JsonInputFormat.class);
		job.setMapperClass(MapperClass.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}