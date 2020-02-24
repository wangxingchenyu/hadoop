package com.zhileiedu.hadoop.myoutputformat.localFile;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 15:38
 */
public class MyRecodWriterLocal extends RecordWriter<LongWritable, Text> {
	/**
	 * 传递给本地系统 windows系统里面
	 */
	private FileOutputStream atguigu;
	private FileOutputStream other;


	public void initialize() throws FileNotFoundException {
		atguigu = new FileOutputStream(new File("d:\\guigu.log"));
		other = new FileOutputStream("d:\\other.log");
	}

	@Override
	public void write(LongWritable key, Text value) throws IOException, InterruptedException {
		// 流初始化
		this.initialize();
		String s = value.toString();
		if (s.contains("atguigu")) {
			atguigu.write(s.getBytes());
		} else {
			other.write(s.getBytes());
		}

	}


	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(atguigu);
		IOUtils.closeStream(other);
	}
}
