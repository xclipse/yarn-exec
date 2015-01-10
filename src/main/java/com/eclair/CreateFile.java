package com.eclair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CreateFile {
  public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    conf.set("dfs.default.name", "hdfs://h1");
    Path output = new Path("/output/");
    Path in = new Path("/in");
    FileSystem fs = FileSystem.get(conf);
    System.out.println(output.getName() + " exist = " + fs.exists(output) + " isFolder = " + fs.isDirectory(output));
    System.out.println(in.getName() + " exist = " + fs.exists(in) + " isFolder = " + fs.isDirectory(in));
    fs.mkdirs(output);
    if(fs.exists(output)){
      fs.delete(output, true);
    }


    Job job = Job.getInstance(conf, "Ecchang map red");
    job.setJarByClass(CreateFile.class);
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setCombinerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, output);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class MyMapper extends Mapper<Text, Text, Text, IntWritable>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException,
        InterruptedException {
      context.write(key, new IntWritable(Integer.parseInt(value.toString())));
    }
  }

  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
        InterruptedException {
      int sum = 0;
      for(IntWritable iw : values){
        sum += iw.get();
      }
      ctx.write(key, new IntWritable(sum));
    }
  }

  public static class MyInputFormat extends KeyValueTextInputFormat{
  }

}
