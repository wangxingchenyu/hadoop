package com.zhileiedu.hadoop.myoutputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 15:36
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {
	@Override
	public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		MyRecodWriter myRecodWriter = new MyRecodWriter();
		myRecodWriter.initialize(job);
		return myRecodWriter;
	}
}
