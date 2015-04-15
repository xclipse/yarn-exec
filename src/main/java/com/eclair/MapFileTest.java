package com.eclair;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapFileTest extends Configured implements Tool {
	private final static Log LOG = LogFactory.getLog(MapFileTest.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if(conf.getBoolean("read", false)){
			return read(args);
		}
		Writer.setIndexInterval(conf, 1);
		Writer writer = new MapFile.Writer(conf, new Path(args[0]),
				Writer.keyClass(Text.class), Writer.valueClass(Text.class),
				Writer.compression(CompressionType.BLOCK));
		writer.setIndexInterval(1);
		Text k = new Text();
		Text v = new Text();
		for (int i = 0; i < 100; i++) {
			if(i == 50) continue;
			k.set("key" + String.format("%1$03d", i));
			v.set("value" + String.format("%1$03d", i));
			writer.append(k, v);
		}
		IOUtils.closeStream(writer);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MapFileTest(), args));
	}

	public int read(String[] args) throws IllegalArgumentException, IOException{
		Configuration conf = getConf();
		Writer.setIndexInterval(conf, 1);
		Text k = new Text();
		Text v = new Text();
		Reader reader = new MapFile.Reader(new Path(args[0]), conf);
		k.set("key" + String.format("%1$03d", 50));
		reader.getClosest(k, v);
		LOG.info(k.toString() + " = " + v);
		v.set("");
		k.set("key" + String.format("%1$03d", 50));
		reader.get(k, v);
		LOG.info(k.toString() + " = " + v);
		IOUtils.closeStream(reader);

		return 0;

	}

}
