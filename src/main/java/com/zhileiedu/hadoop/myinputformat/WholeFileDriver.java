package com.zhileiedu.hadoop.myinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 15:39
 */
public class WholeFileDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());

		job.setJarByClass(WholeFileDriver.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// 设置输入格式
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 重点提示，如果	com.zhileiedu.hadoop.myinputformat.MyInputFormat 不是继承的是FileInputFormat 则下面不能直接使用FileInputFormat
		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\output"));

		boolean b = job.waitForCompletion(true);

		//  完成则输出0，否则则输出1
		System.exit(b ? 0 : 1);
	}
}
