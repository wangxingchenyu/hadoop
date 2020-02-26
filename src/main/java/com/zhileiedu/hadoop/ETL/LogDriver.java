package com.zhileiedu.hadoop.ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/25 12:07
 */
public class LogDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());

		job.setJarByClass(LogDriver.class);

		job.setMapperClass(LogMapper.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\output"));
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
