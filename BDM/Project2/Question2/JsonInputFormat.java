import java.io.IOException;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

public class JsonInputFormat extends InputFormat<LongWritable, Text>
{
	public Pattern[] pattern={Pattern.compile("\\{"),Pattern.compile("\\},*")};
	private TextInputFormat t=new TextInputFormat();
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,InterruptedException
	{
		return t.getSplits(context);
	}
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,TaskAttemptContext arg1) throws IOException, InterruptedException 
	{
		JsonRecordReader reader = new JsonRecordReader();
		if (pattern == null)
		{
			System.out.println("Unable to create RecordReader");
			System.exit(1);
		}
		reader.setPattern(pattern);
		return reader;
	}
	public class JsonRecordReader extends RecordReader<LongWritable, Text>
	{
		private LineRecordReader lineRecordReader = new LineRecordReader();
		private Pattern[] pattern;
		Text value = new Text();
		public void setPattern(Pattern[] patt)
		{
			this.pattern = patt;
		}
		@Override
		public void close() throws IOException 
		{
			lineRecordReader.close();
		}
		@Override
		public LongWritable getCurrentKey() throws IOException,InterruptedException 
		{
			return lineRecordReader.getCurrentKey();
		}
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException 
		{
			return value;
		}
		@Override
		public float getProgress() throws IOException, InterruptedException 
		{
			return lineRecordReader.getProgress();
		}
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException 
		{
			lineRecordReader.initialize(genericSplit, context);
		}
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException 
		{
			Text res = new Text();
			StringBuilder sb = new StringBuilder();
			boolean found = false;
			Text value = new Text();
			while(lineRecordReader.nextKeyValue()) 
			{
				Matcher matcher1, matcher2;
				String line = lineRecordReader.getCurrentValue().toString();
				matcher1 = pattern[0].matcher(line);
				matcher2 = pattern[1].matcher(line);
				if (matcher1.find()) 
					found = true;
				if (found)
				{
					String s = lineRecordReader.getCurrentValue().toString().replaceAll("(\\{)|(\\},*)|([^:]*:\\s*)", 	"");
					s = s.replaceAll("[^:]*:\\s*", "");
					if (s.length() > 0) 
						sb.append(s);
					if (matcher2.find()) 
					{
						res.set(sb.toString());
						value.append(res.getBytes(), 0, res.getLength());
						return true;
					}
				}
			}
			return false;
		}
	}
}