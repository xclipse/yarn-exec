package com.eclair;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MultipleOutputDemo extends Configured implements Tool{

	private final static Log LOG = LogFactory.getLog(MultipleOutputDemo.class);
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			return -1;
		}
		Job job = Job.getInstance(getConf());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setJarByClass(getClass());
		job.setMapperClass(OneMapper.class);
		//MultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, keyClass, valueClass);

		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new MultipleOutputDemo(), args);
		System.exit(exit);
	}


	public static class OneMapper extends Mapper<Text, Text, NullWritable, NullWritable>{
		MultipleOutputs<NullWritable, NullWritable> mo;
		@Override
		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mo = new MultipleOutputs<NullWritable, NullWritable>(context);
		}
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text k = new Text("[" +key.toString() + "]");
			mo.write(key.toString(), NullWritable.get(), value);

		}
	}
}
