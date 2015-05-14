package com.eclair;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSort extends Configured implements Tool {

	private final static Log LOG = LogFactory.getLog(TotalSort.class);
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 1){
			System.err.printf("Usage: %s [generic options] <output>\n", getClass().getSimpleName());
			return -1;
		}

		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		LOG.info(" mapreduce.job.ubertask.enable = " + conf.get("mapreduce.job.ubertask.enable"));
		Path outPath = new Path(args[0]);
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}

		Path tmp = new Path("/tmp");

		TotalOrderPartitioner.setPartitionFile(conf, new Path(tmp, "partition.txt"));
		LOG.info(" TotalOrderPartitioner.getPartitionFile(conf) = " + TotalOrderPartitioner.getPartitionFile(conf));

		Path input = new Path(tmp, "RandomInput.txt");
		org.apache.hadoop.io.SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(input),
				SequenceFile.Writer.keyClass(IntWritable.class),
				SequenceFile.Writer.valueClass(Text.class));

		IntWritable key = new IntWritable();
		Text val = new Text();
		key.set(0);
		Random r = new Random();
		for (int i = 0; i < 1000; i++) {
			key.set(r.nextInt(100));
			val.set("Line... " + String.format("%1$04d", i));
			writer.append(key, val);
		}

		IOUtils.closeStream(writer);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setJarByClass(getClass());
		job.setMapperClass(TotalSortMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);
		InputSampler.RandomSampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 50, 3);
		InputSampler.writePartitionFile(job, sampler);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new TotalSort(), args);
		System.exit(exit);
	}

	public static class TotalSortMapper extends Mapper<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key ,value);
		}
	}
}
