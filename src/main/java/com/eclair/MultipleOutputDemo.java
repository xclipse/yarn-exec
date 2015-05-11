package com.eclair;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MultipleOutputDemo extends Configured implements Tool{

	private final static Log LOG = LogFactory.getLog(MultipleOutputDemo.class);

	enum NamedOutput {
		seq,text,map;
	}
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
		MultipleOutputs.addNamedOutput(job, NamedOutput.seq.toString(), SequenceFileOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, NamedOutput.text.toString(), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, NamedOutput.map.toString(), MapFileOutputFormat.class, IntWritable.class, Text.class);


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
			context.getCounter(NamedOutput.map).setValue(0);
		}
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String baseOutputPath = key.toString();
			String namedOutput = baseOutputPath.split("/")[0];
			switch(NamedOutput.valueOf(namedOutput)){
			case seq:
				mo.write(namedOutput, key, value, baseOutputPath);
				break;
			case text:
				mo.write(namedOutput, NullWritable.get(), value, baseOutputPath);
				break;
			case map:
				Counter ct = context.getCounter(NamedOutput.map);
				ct.increment(10);
				mo.write(namedOutput, new IntWritable((int) ct.getValue()), value, baseOutputPath);
				break;
			}
		}
	}
}
