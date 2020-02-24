package com.zhileiedu.hadoop.myoutputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 15:38
 */
public class MyRecodWriter extends RecordWriter<LongWritable, Text> {
	private FSDataOutputStream atguigu;
	private FSDataOutputStream other;


	public void initialize(TaskAttemptContext job) throws IOException {
		// 查询配置的信息，查到输出的路径是多少
		String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
		// 里面有相关的配置的参数
		FileSystem fs = FileSystem.get(job.getConfiguration());
		// 创建输入的文档，如果是open的话，则是创建输入流，等待文件写入，此处理为文件写出
		atguigu = fs.create(new Path(outDir + "/atguigu.log"));
		other = fs.create(new Path(outDir + "/other.log"));
	}

	@Override
	public void write(LongWritable key, Text value) throws IOException, InterruptedException {
		// 流初始化
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
