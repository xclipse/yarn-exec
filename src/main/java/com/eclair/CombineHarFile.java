package com.eclair;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// command line: hadoop jar test.jar com.eclair.CombineHarFile har:///har/files.har /out
public class CombineHarFile extends Configured implements Tool {
	private final static Log LOG = LogFactory.getLog(CombineHarFile.class);

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new CombineHarFile(), args);
		System.exit(exit);
	}

	enum Counter {
		MAPPER, REDUCE, TOTAL
	}


	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			return -1;
		}
		Configuration conf = getConf();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "h1");

		Job job = Job.getInstance(getConf());
		job.setJobName("Combine Text Input Format");
		System.out.println("  ================== mapreduce.map.log.level ="
				+ job.getConfiguration().get("mapreduce.map.log.level"));
		FileSystem fs = FileSystem.get(conf);

		// Path path = new Path("har:///my/files.har");
		LOG.info("ecchang  MinSplitSize = " + CombineTextInputFormat.getMinSplitSize(job));
		LOG.info("ecchang  MaxSplitSize = " + CombineTextInputFormat.getMaxSplitSize(job));
		//CombineTextInputFormat.setMinInputSplitSize(job, 1024^3);
		CombineTextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(CombineTextInputFormat.class);

		job.setJarByClass(getClass());
		job.setMapperClass(CombineMapper.class);
		job.setReducerClass(CombineReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class CombineMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			CombineFileSplit cfs = (CombineFileSplit)context.getInputSplit();
			LOG.info(" ECCHANGE mapper setup -- getNumPaths = " + cfs.getNumPaths() );
			LOG.info(" ECCHANGE mapper setup -- check Paths +++ " + cfs);
		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			CombineFileSplit cfs = (CombineFileSplit)context.getInputSplit();
			Text k = new Text("[" + key.toString() + "]");
			context.getCounter(Counter.MAPPER).increment(1);
			context.getCounter(Counter.TOTAL).increment(1);
			if (context.getCounter(Counter.MAPPER).getValue() == 1) {
				context.setStatus("ECLAIR 1st mapper start to work");
			}
			LOG.info("This is LOG: current count is "
					+ context.getCounter(Counter.MAPPER).getValue());
			System.out.println("ECLAIR This is system out: current count is "
					+ context.getCounter(Counter.MAPPER).getValue());
			context.write(k, value);
		}
	}

	public static class CombineReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(key, text);
			}
		}
	}
}
