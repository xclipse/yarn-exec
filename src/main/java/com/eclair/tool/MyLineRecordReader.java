/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.eclair.tool;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class MyLineRecordReader extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(MyLineRecordReader.class);
	LineRecordReader lrr = new LineRecordReader();

	public MyLineRecordReader() {
	}
	public MyLineRecordReader(LineRecordReader lineRecordReader) {
		lrr=lineRecordReader;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		LOG.debug("DEBUG LEVEL ECCHANG MyLineRecordReader.initialize");
		LOG.info("INFO LEVEL ECCHANG MyLineRecordReader.initialize");
		lrr.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lrr.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lrr.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lrr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lrr.getProgress();
	}

	@Override
	public void close() throws IOException {
		lrr.close();
	}}
