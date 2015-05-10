package com.eclair.tool;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.google.common.base.Charsets;

public class MyInputFormat extends FileInputFormat<LongWritable, Text> {
	TextInputFormat tif = new TextInputFormat();
	private static final Log LOG = LogFactory.getLog(MyInputFormat.class);
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		LOG.info(" ECCHANG DEBUG ------------ MyInputFormat.createRecordReader");

	    String delimiter = context.getConfiguration().get(
	        "textinputformat.record.delimiter");
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter)
	      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
	    return new MyLineRecordReader(new LineRecordReader(recordDelimiterBytes));
	}

}
