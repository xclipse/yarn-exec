package com.eclair;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
public class HARFile {

  public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Path output = new Path("file3.txt");
    Path in = new Path("har:///har/file.har/file1.txt");
    FileSystem fs = FileSystem.get(new URI("har:///har/file.har") , conf);
    System.out.println(output.getName() + " exist = " + fs.exists(output) + " isFolder = " + fs.isDirectory(output));
    System.out.println(in.getName() + " exist = " + fs.exists(in) + " isFolder = " + fs.isDirectory(in));
    FSDataInputStream ins = fs.open(in);
    byte[] bytes = new byte[1024];
    ByteBuffer buf = ByteBuffer.allocate(1024);
    int i = ins.read(bytes);
    System.out.print(new String(bytes,0,i));
//    while(ins.read(buf) > 0){
//      buf.flip();
//      while(buf.hasRemaining()){
//        buf.get(bytes);
//        System.out.print(new String(bytes));
//      }
//    }
  }

}
