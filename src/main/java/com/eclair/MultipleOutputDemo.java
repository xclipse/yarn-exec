package com.eclair;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
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



		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		MultipleOutputs.addNamedOutput(job, NamedOutput.seq.toString(), SequenceFileOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, NamedOutput.text.toString(), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, NamedOutput.map.toString(), MapFileOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.setCountersEnabled(job, true);

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
		protected void cleanup(
				Mapper<Text, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			mo.close();
		}
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String baseOutputPath = key.toString();
			LOG.info("baseOutputPath = " + baseOutputPath);
			String namedOutput = baseOutputPath.split("/")[0];
			LOG.info("namedOutput = " + namedOutput);
			LOG.info(" MAP_INPUT_RECORDS = " + context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());

 			NamedOutput valueOf = null;
			try {
				valueOf = NamedOutput.valueOf(namedOutput);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
			Counter ct;
			switch(valueOf){
			case seq:
				ct = context.getCounter(NamedOutput.seq);
				ct.increment(1);
				mo.write(namedOutput, key, value, baseOutputPath);
				break;
			case text:
				ct = context.getCounter(NamedOutput.text);
				ct.increment(1);
				mo.write(namedOutput, NullWritable.get(), value, baseOutputPath);
				break;
			case map:
				ct = context.getCounter(NamedOutput.map);
				ct.increment(1);
				LOG.info(" TaskAttemptID =" + context.getTaskAttemptID() +  " Map Counter = " + ct.getValue());
				mo.write(namedOutput, new IntWritable((int) ct.getValue()), value, baseOutputPath);
				break;
			default:
				break;
			}
		}

	}
}
