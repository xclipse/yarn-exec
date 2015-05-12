package com.eclair;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartialSort extends Configured implements Tool {

	private final static Log LOG = LogFactory.getLog(PartialSort.class);
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2 && args.length != 3){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			return -1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		conf.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}

		job.setOutputFormatClass(MapFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		//SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.setJarByClass(getClass());
		job.setMapperClass(SortMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		int i = job.waitForCompletion(true) ? 0 : 1;
		if(args.length > 2){
			FileSystem fs = FileSystem.get(conf);
			RemoteIterator<LocatedFileStatus> lf = fs.listFiles(outPath, false);
			while(lf.hasNext()){
				LocatedFileStatus fst = lf.next();
				fst.isFile();
				fs.delete(fst.getPath(), false);
			}
			Writable value = new Text();
			WritableComparable<?> key = new IntWritable(Integer.valueOf(args[2]));
			lookup(conf, args[1], key, value, key.getClass());
		}
		return i;
	}


	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new PartialSort(), args);
		System.exit(exit);
	}

	public static class SortMapper extends Mapper<Text, Text, IntWritable, Text>{
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(Integer.valueOf(key.toString())),value);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static <K extends WritableComparable<?> , V extends Writable> int lookup(Configuration conf, String mapPath, K key, V value,  Class<? extends WritableComparable> clazz) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		Path path = new Path(mapPath);
		Reader[] readers = MapFileOutputFormat.getReaders(path, conf);
		Partitioner<K, V> partitioner = new HashPartitioner<K, V>();
		//MapFileOutputFormat.getEntry(readers, partitioner, key, value);
		Reader reader = readers[partitioner.getPartition(key, value, readers.length)];
		if(reader.get(key, value) == null){
			LOG.info(" No Key Found ---");
			return -1;
		}
		Constructor<? extends WritableComparable> c = clazz.getConstructor();
		WritableComparable<?> newKey = c.newInstance();
		do{
			LOG.info("Find Key = " + key + " value = " + value);
		}while(reader.next(newKey, value) && newKey.equals(key));
		return 0;
	}
}
