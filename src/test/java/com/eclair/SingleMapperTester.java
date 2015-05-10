package com.eclair;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class SingleMapperTester {

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void validProcesstest() throws IOException {
    new MapDriver<Text, Text, Text, Text>().withMapper(new SingleMapper.OneMapper())
    .withInput(new Text("INPUT"), new Text("VALUE"))
    .withOutput(new Text("[INPUT]"), new Text("VALUE"))
    .runTest();
  }

  @Test
  public void simpleTest(){
	  int i = 1;

	  do{
		  System.out.println(i ++);
		  if(i < 3){
			  continue;
		  }
	  }while (false);

  }
}
