package com.eclair;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SingleMapper extends Configured implements Tool {

	private final static Log LOG = LogFactory.getLog(SingleMapper.class);
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			return -1;
		}

		// Wrong Sample...............
//		Configuration conf = getConf();
//		Job job = Job.getInstance(conf);

		// get conf from job
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
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setJarByClass(getClass());
		job.setMapperClass(OneMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new SingleMapper(), args);
		System.exit(exit);
	}

	public static class OneMapper extends Mapper<Text, Text, Text, Text>{
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Text k = new Text("[" +key.toString() + "]");
			context.write(k,value);
		}
	}
}
