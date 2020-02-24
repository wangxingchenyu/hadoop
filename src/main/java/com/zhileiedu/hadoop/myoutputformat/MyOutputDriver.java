package com.zhileiedu.hadoop.myoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 18:24
 */
public class MyOutputDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(MyOutputDriver.class);

		job.setOutputFormatClass(MyOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\output"));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}
