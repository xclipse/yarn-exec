package com.eclair;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GZTest extends Configured implements Tool {
	private final static Log LOG = LogFactory.getLog(GZTest.class);
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		LOG.info("decompress = " +conf.get("decompress"));

		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);

		CompressionCodec codec =  ReflectionUtils.newInstance(GzipCodec.class, conf );
		
		if("Y".equals(conf.get("decompress"))){
			LOG.info("Start to decompress ... ");
			Decompressor dc = CodecPool.getDecompressor(codec);
			FSDataInputStream in = fs.open(new Path(args[0]));
			FSDataOutputStream out = fs.create(new Path(args[1]));
			try {
				CompressionInputStream ic = codec.createInputStream(in, dc);
				IOUtils.copyBytes(ic, out, conf);
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
				CodecPool.returnDecompressor(dc);
			}
		} else {
			FSDataInputStream in = fs.open(new Path(args[0]));
			FSDataOutputStream out = fs.create(new Path(args[1]));
			Compressor c = CodecPool.getCompressor(codec,conf);
			try {
				CompressionOutputStream oc = codec.createOutputStream(out, c);
				IOUtils.copyBytes(in, oc, conf);
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				LOG.info(CodecPool.getLeasedCompressorsCount(codec));
				CodecPool.returnCompressor(c);
				LOG.info(CodecPool.getLeasedCompressorsCount(codec));
			}
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GZTest(), args));

	}

}
