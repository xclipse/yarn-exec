package com.eclair;

import java.awt.Toolkit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceWriteLocal extends Configured implements Tool  {

  public static void main(String[] args) throws Exception {

    System.exit(ToolRunner.run(new SequenceWriteLocal(), args));
  }

  @Override
  public int run(String[] arg0) throws Exception {
    
    SequenceFileInputFormat<String, String> s = new SequenceFileInputFormat<String, String>();
    SequenceFileOutputFormat<String, String> e = new SequenceFileOutputFormat<String, String>();
    Configuration conf = this.getConf(); 
    Job job = Job.getInstance(conf);
    e.setOutputCompressionType(job, CompressionType.BLOCK);
    return 0;
  }

}
