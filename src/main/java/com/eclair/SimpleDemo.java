package com.eclair;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleDemo extends Configured implements Tool{

  private final static Log LOG = LogFactory.getLog(SimpleDemo.class);

  //hadoop jar test/test.jar com.eclair.SimpleDemo -D mapreduce.map.log.level=DEBUG /in /out
  @Override
  public int run(String[] args) throws Exception {
    if(args.length != 2){
      System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      return -1;
    }
    Configuration conf = getConf();
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.hostname", "h1");
    conf.set("fs.defaultFS", "hdfs://h1");
    ControlledJob cjob =new ControlledJob(getConf());
    Job job = cjob.getJob();
    job.setJobName("Simple Mapper");
    System.out.println(" ================== mapreduce.map.log.level =" + job.getConfiguration().get("mapreduce.map.log.level"));
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/out");
    if(fs.exists(path)){
    	fs.delete(path, true);
    }

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setInputFormatClass(TextInputFormat.class); // default input format

    job.setJarByClass(getClass());
    job.setMapperClass(SimpleMapper.class);
    job.setReducerClass(SimpleReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //job.setNumReduceTasks(0);
    JobControl jc = new JobControl("groupname");
	jc.addJob(cjob);
	LOG.info("jc.getReadyJobsList() = " +jc.getReadyJobsList().size());
	LOG.info("jc.getRunningJobList() = " +jc.getRunningJobList().size());
	new Thread(jc).start();
	while(true){
		if(jc.allFinished()){
			LOG.info("jc.getSuccessfulJobList() =" + jc.getSuccessfulJobList());
			jc.stop();
			return 0;
		}
		if(jc.getFailedJobList().size() > 0){
			LOG.info("jc.getFailedJobList() =" + jc.getFailedJobList());
			jc.stop();
			return -1;
		}
		LOG.info("Sleep 20 sec");
		LOG.info("jc.getReadyJobsList() = " +jc.getReadyJobsList().size());
		LOG.info("jc.getRunningJobList() = " +jc.getRunningJobList().size());
		TimeUnit.SECONDS.sleep(20);
	}
  }

  public static void main(String[] args) throws Exception {

    int exit = ToolRunner.run(new SimpleDemo(), args);
    System.exit(exit);
  }

  enum Counter{
    MAPPER,REDUCE,TOTAL
  }
  public static class SimpleMapper extends Mapper<LongWritable, Text, Text, Text>{
    public SimpleMapper() {
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      Text k = new Text("[" +key.toString() + "]");
      context.getCounter(Counter.MAPPER).increment(1);
      context.getCounter(Counter.TOTAL).increment(1);
      if(context.getCounter(Counter.MAPPER).getValue() == 1){
        context.setStatus("ECLAIR 1st mapper start to work");
      }

      LOG.debug("This is LOG: current count is " + context.getCounter(Counter.MAPPER).getValue());
      System.out.println("ECLAIR This is system out: current count is " + context.getCounter(Counter.MAPPER).getValue());
      context.write(k,value);
    }
  }

  public static class SimpleReduce extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException,
        InterruptedException {
      context.getCounter(Counter.REDUCE).increment(1);
      context.getCounter(Counter.TOTAL).increment(1);
      if(context.getCounter(Counter.MAPPER).getValue() == 1){
        context.setStatus("1st mapper start to work");
      }
      LOG.debug("ECLAIR This is LOG: current count is " + context.getCounter(Counter.MAPPER).getValue());
      System.out.println("ECLAIR This is system out: current count is " + context.getCounter(Counter.MAPPER).getValue());
      for (Text text : values) {
        context.write(key,text);
      }
    }



  }
}
