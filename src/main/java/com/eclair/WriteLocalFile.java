package com.eclair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class WriteLocalFile {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
	    //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    conf.set("dfs.defaultFS", "hdfs://h1");
	    Path outpath = new Path("/tmp/file.txt");
	    Path inpath = new Path("/in/file1.txt");

	    LocalFileSystem outfs = FileSystem.getLocal(conf);
	    FileSystem infs = FileSystem.get(conf);
	    FSDataOutputStream out = outfs.create(outpath);
	    FSDataInputStream in = infs.open(inpath);

	    IOUtils.copyBytes(in, out, 1024, true);

	}

}
