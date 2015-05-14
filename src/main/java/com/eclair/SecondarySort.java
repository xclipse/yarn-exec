package com.eclair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySort extends Configured implements Tool {

	private final static Log LOG = LogFactory.getLog(SecondarySort.class);
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			return -1;
		}

		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();

		LOG.info(" mapreduce.job.ubertask.enable = " + conf.get("mapreduce.job.ubertask.enable"));
		Path outPath = new Path(args[1]);
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		job.setJobName("Single Mapper");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(getClass());
		job.setMapperClass(OneMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new SecondarySort(), args);
		System.exit(exit);
	}

	static class ComponentComparator extends WritableComparator{

	}

	static class ComponentWritable implements WritableComparable<WritableComparable<?>>{

		List<ComponentWritable> components = new ArrayList<SecondarySort.ComponentWritable>();
		WritableComparable<WritableComparable<?>> value;

		@Override
		public void write(DataOutput out) throws IOException {

		}

		@Override
		public void readFields(DataInput in) throws IOException {

		}

		@Override
		public int compareTo(WritableComparable<?> o) {
			return value.compareTo(o);
		}

	}
	public static class OneMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//context.write(k,value);
		}
	}
}
