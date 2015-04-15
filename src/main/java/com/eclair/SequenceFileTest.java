package com.eclair;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileTest extends Configured implements Tool {
	private final static Log LOG = LogFactory.getLog(SequenceFileTest.class);
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SequenceFileTest(), args));
	}

	// hadoop jar test/test.jar com.eclair.SequenceFileTest file:///home/hadoop/file.seq
	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = getConf();
		if(conf.getBoolean("read",false)){
			return read(arg);
		}
		Path path = new Path(arg[0]);
		CompressionCodec codec = new GzipCodec();
		Writer writer = SequenceFile.createWriter(conf, Writer.file(path),
				Writer.compression(SequenceFile.getDefaultCompressionType(conf)),
				Writer.keyClass(IntWritable.class),
				Writer.valueClass(Text.class));
		LOG.info("writer.getCompressionCodec() = " + writer.getCompressionCodec().getClass() );
		IntWritable iw = new IntWritable();
		Text t = new Text();
		for (int i = 0; i < 100; i++) {
			if(i%10 == 0 ){
				writer.sync();
			}
			iw.set(i);
			t.set("Line" + i);
			writer.append(iw, t);
		}
		IOUtils.closeStream(writer);
		return 0;
	}


	// hadoop jar test/test.jar com.eclair.SequenceFileTest -D read=true  file:///home/hadoop/file.seq
	public int read(String[] arg) throws IOException{
		Configuration conf = getConf();
		Path path = new Path(arg[0]);
		Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		LOG.info("reader.isCompressed() = " + reader.isCompressed());
		IntWritable iw = new IntWritable();
		Text t = new Text();
		LOG.info(" Sequence File context: ");
		long position = reader.getPosition();
		while(reader.next(iw, t)){
			LOG.info((reader.syncSeen()?"*":" ") + " Position = " + position +" Key = " + iw.get() + " value = " + t.toString());
			position = reader.getPosition();
		}
		IOUtils.closeStream(reader);
		return 0;
	}
}
